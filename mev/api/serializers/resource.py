import logging

from django.contrib.auth import get_user_model
from rest_framework import serializers, exceptions

from api.models import Resource, Workspace
from api.utilities.resource_utilities import set_resource_to_inactive
from api.serializers.workspace import WorkspaceSerializer
import api.async_tasks.async_resource_tasks as api_tasks
from api.storage_backends import get_storage_backend

from resource_types import DB_RESOURCE_STRING_TO_HUMAN_READABLE

logger = logging.getLogger(__name__)

class ResourceSerializer(serializers.ModelSerializer):

    # add a human-readable datetime
    created = serializers.DateTimeField(
        source='creation_datetime', 
        read_only=True
    )
    owner_email = serializers.EmailField(source='owner.email', required=False, allow_null=True)
    workspaces = WorkspaceSerializer(many=True, required=False, read_only=True)
    path = serializers.CharField(write_only=True, required=False)
    size = serializers.IntegerField(read_only=True)
    readable_resource_type = serializers.SerializerMethodField()

    class Meta:
        model = Resource
        fields = [
            'id',
            'url',
            'name',
            'resource_type',
            'owner_email',
            'is_active',
            'is_public',
            'status',
            'workspaces',
            'created',
            'path',
            'size',
            'readable_resource_type'
        ]

    def get_readable_resource_type(self, obj):
        if obj.resource_type:
            return DB_RESOURCE_STRING_TO_HUMAN_READABLE[obj.resource_type]
        else:
            return None

    @staticmethod
    def parse_request_parameters(validated_data):
        '''
        This is a helper function to parse out the various 
        optional parameters from the request to create or
        edit a Resource
        '''
        path = validated_data.get('path', '')
        name = validated_data.get('name', '')
        workspaces = validated_data.get('workspaces', None),
        resource_type = validated_data.get('resource_type', None)
        is_public = validated_data.get('is_public', False)
        is_active = validated_data.get('is_active', False)
        status = validated_data.get('status', '')
        size = validated_data.get('size', 0)

        # the 'get' above returns a tuple so we have to index
        # to get the actual workspace instance or None
        #workspace = workspace[0]

        return {
            'path': path,
            'name': name,
            'workspaces': workspaces, 
            'resource_type': resource_type,
            'is_public' : is_public,
            'is_active' : is_active,
            'status': status,
            'size': size
        }

    def create(self, validated_data): 

        # the user who generated the request.  A User instance (or subclass)
        # who MUST be an admin.  Regular users cannot create `Resources`
        # via API calls.
        # If we are using the serializer to validate data when creating
        # Resources internally, there will not be a `requesting_user` key.

        logger.info('Received validated data: %s' % validated_data)
        
        internal_call = False
        try:
            requesting_user = validated_data['requesting_user']
        except KeyError:
            internal_call = True

        try:
            owner_email = validated_data['owner']['email']
        except KeyError as ex:
            raise exceptions.ParseError({'owner_email':'This field'
                ' must be supplied with the payload.'
            })

        # check that the owner does exist, if given.  If not, return an error
        if owner_email:
            try:
                resource_owner = get_user_model().objects.get(email=owner_email)
            except get_user_model().DoesNotExist as ex:
                logger.info('User %s did not exist.' % owner_email)
                raise exceptions.ParseError()
        else:
            resource_owner = None

        # If the user is an admin, they can create a Resource for anyone.
        # The DRF permissions should catch problems before here, but this is
        # extra insurance
        if internal_call or requesting_user.is_staff:
            params = ResourceSerializer.parse_request_parameters(validated_data)

            resource = Resource.objects.create(
                owner=resource_owner,
                path=params['path'],
                name=params['name'],
                resource_type=params['resource_type'],
                is_public=params['is_public'],
                size=params['size']
            )                
            logger.info('Created a Resource: %s' % resource)
            return resource
        else:
            raise exceptions.PermissionDenied()

    def update(self, instance, validated_data):
        '''
        When a user updates a Resource, they can only do a few things--
        - change the name
        - change the resource type (which triggers a type validation)

        In addition to these, admins may also change:
        - status
        - path
        - change public or private
        '''

        logger.info('Received validated data: %s' % validated_data)

        # if we are performing validation, or some other action has 
        # set the resource "inactive", we cannot make changes.  Issue an error 
        if not instance.is_active:
            logger.info('Change was requested on inactive Resource %s' % instance)
            raise exceptions.ParseError('The requested Resource'
            ' is not currently activated, possibly due to pending'
            ' validation.  Please wait and try again.')

        try:
            requesting_user = validated_data['requesting_user']
        except KeyError as ex:
            logger.info('The "requesting_user" was not supplied to the update method.')
            raise ex

        # we could just ignore any data attempting to
        # change the owner of the Resource, but
        # we will explicitly reject any attempts.
        original_owner_email = instance.owner.email
        new_requested_owner = validated_data.get('owner', None)
        if new_requested_owner:
            new_owner_email = new_requested_owner['email']
            if original_owner_email != new_owner_email:
                raise exceptions.ParseError({'owner_email':'Cannot change the owner of a workspace.'})

        # Note that we cannot change the workspace with this method.
        # However, providing a workspace is not an error provided the 
        # workspace (if assigned) happens to be the same.
        # We could ignore requests to change the associated workspace, but it is
        # more helpful to issue a message.
        if validated_data.get('workspaces', None):
            logger.info('A change in Workspace was requested.  Rejecting this request.')
            raise exceptions.ParseError({'workspaces':'Cannot change the workspaces.'
                ' Use the API methods to add or remove from the respective Workspace(s).'}
            )

        # only admins are allowed to change public/private status
        is_public = validated_data.get('is_public', None)
        if (is_public is not None) and (not requesting_user.is_staff):
            logger.info('Regular user requesting change of public/private status')
            raise exceptions.ParseError({'is_public':'Cannot change the public/private'
                ' status of a resource.'}
            )

        # the following are fields able to be edited by regular users and admins:
        instance.name = validated_data.get('name', instance.name)

        # fields that can only be edited by admins:
        if requesting_user.is_staff:
            instance.is_public = validated_data.get('is_public', instance.is_public)
            instance.status = validated_data.get('status', instance.status)
            instance.path = validated_data.get('path', instance.path)

        # Now handle any requests to change the resource type
        changing_resource_type = False
        new_resource_type = validated_data.get('resource_type', None)
        if new_resource_type:
            # if the type is different than the existing assigned type, reset the flag
            if new_resource_type != instance.resource_type:
                # Until the validation is complete we simply change the status and save.  
                # Note that the resource_type has NOT changed yet.  
                # 
                # If the validation succeeds, then the `resource_type` and
                # that status will be changed.
                #
                # Also note that we change the status immediately since the async
                # task might lag and not be executed immediately.
                logger.info('A new resource type was requested.  '
                    'Changing %s from %s to %s' % (
                        str(instance.pk), instance.resource_type, new_resource_type
                    )
                )
                changing_resource_type = True
                set_resource_to_inactive(instance)

        # if the `resource_type` was changed in the request, start
        # the validation process.  Since it is calling an async, we
        # have to pass the primary key instead of the instance.
        if changing_resource_type:
            logger.info('Queueing validation for updating resource %s with type %s ' % 
                (str(instance.pk), new_resource_type)
            )
            instance.status = Resource.VALIDATING
            api_tasks.validate_resource.delay(
                instance.pk, 
                new_resource_type
            )

        instance.save()
        return instance
        