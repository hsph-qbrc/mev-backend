import logging
import uuid

from django.conf import settings
from django.core.files import File
from django.core.files.storage import default_storage
from rest_framework import permissions as framework_permissions
from rest_framework import generics
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response

from exceptions import NonIterableContentsException, \
    OwnershipException, \
    InactiveResourceException, \
    NoResourceFoundException, \
    ParseException

from api.models import Resource, Workspace
from api.serializers.resource import ResourceSerializer
import api.permissions as api_permissions
from api.utilities.resource_utilities import resource_supports_pagination, \
    check_resource_request_validity, \
    create_resource, \
    initiate_resource_validation, \
    retrieve_resource_class_standard_format
from api.utilities.basic_utils import read_local_file
from resource_types import get_resource_type_instance
from api.data_transformations import get_transformation_function
from api.async_tasks.async_resource_tasks import \
    delete_file as async_delete_file
from api.async_tasks.async_resource_tasks import \
    validate_resource as async_validate_resource

logger = logging.getLogger(__name__)


def check_resource_request(user, resource_pk):
    '''
    Helper function that asserts valid access to a Resource.

    Returns a tuple of:
    - bool
    - object

    The bool is True if the request was valid; in that case, the object
    will be an instance of a api.models.AbstractResource (or child class).

    If False, the second item in the tuple will be a DRF Response object 
    appropriate for the failure reason.

    We do this combination so that we don't have to do any type checking 
    in the calling function/method
    '''
    try:
        r = check_resource_request_validity(user, resource_pk)
        return (True, r)
    except OwnershipException:
        return (False, Response(status=status.HTTP_403_FORBIDDEN))
    except InactiveResourceException:
        return (False, Response({
            'resource': 'The requested resource is'
            ' not active.'},
            status=status.HTTP_400_BAD_REQUEST))
    except NoResourceFoundException:
            return (False, Response(status=status.HTTP_404_NOT_FOUND))


class ResourceList(generics.ListAPIView):
    '''
    Lists available Resource instances.
    '''
    
    # permission_classes = [

    #     # regular users need to be authenticated
    #     # AND are only allowed to list Resources.
    #     # They can't add/modify
    #     (framework_permissions.IsAuthenticated 
    #     & 
    #     api_permissions.ReadOnly)
    # ]

    serializer_class = ResourceSerializer

    def get_queryset(self):
        '''
        Note that the generic `permission_classes` applied at the class level
        do not provide access control when accessing the list.  

        This method dictates that behavior.
        '''
        return Resource.objects.select_related('owner').\
            prefetch_related('workspaces').\
            filter(owner=self.request.user)


class ResourceDetail(generics.RetrieveUpdateDestroyAPIView):
    '''
    Retrieves a specific Resource instance.
    
    Users can only view/edit their own Resources.
    '''

    permission_classes = [
        api_permissions.IsOwner & framework_permissions.IsAuthenticated
    ]

    queryset = Resource.objects.all()
    serializer_class = ResourceSerializer

    def perform_update(self, serializer):
        '''
        Adds the requesting user to the request payload
        '''
        serializer.save(requesting_user=self.request.user)

    def destroy(self, request, *args, **kwargs):
        '''
        When we receive a delete/destroy request, we have to ensure we
        are not deleting critical data.

        Thus, if a Resource is associated with one or more Workspaces, then
        no action will happen. 
        '''

        instance = self.get_object()
        logger.info(f'Requesting deletion of Resource: {instance}')

        if not instance.is_active:
            logger.info(f'Resource {instance.pk} was not active.'
                ' Rejecting request for deletion.')
            return Response(status=status.HTTP_400_BAD_REQUEST)

        if len(instance.workspaces.all()) > 0:

            logger.info('Resource was associated with one or more workspaces'
                ' and cannot be removed.')
            return Response(status=status.HTTP_403_FORBIDDEN)

        # at this point, we have an active Resource associated with
        # zero workspaces. delete.
        # delete the actual file
        async_delete_file.delay(instance.datafile.name)
        
        # Now delete the database object:
        self.perform_destroy(instance)
        return Response(status=status.HTTP_200_OK)


class ResourceContents(APIView):
    '''
    Returns the full data underlying a Resource.

    Typically used for small files so that user-interfaces can display
    data

    Depending on the data, the format of the response may be different.
    Additionally, some Resource types do not support a preview.
    
    This returns a JSON-format representation of the data.

    This endpoint is only really sensible for certain types of 
    Resources, such as those in table format.  Other types, such as 
    sequence-based files do not have this functionality.
    '''

    def get(self, request, *args, **kwargs):
        user = request.user
        resource_pk=kwargs['pk']

        valid_request, r = check_resource_request(user, resource_pk)
        if not valid_request:
            # if the request was not valid, then `r` is a Response object.
            return r

        # requester can access, resource is active.  Go get contents
        try:
            resource_type = get_resource_type_instance(r.resource_type)
            contents = resource_type.get_contents(r, request.query_params)
            logger.info('Done getting contents.')
        except ParseException as ex:
            return Response(
                {'error': 'There was a problem when parsing'
                         f' the request: {ex}'},
                status=status.HTTP_400_BAD_REQUEST
            )  
        except Exception as ex:
            return Response(
                {'error': 'Experienced an issue when preparing'
                         f' the resource view: {ex}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )   
        if contents is None:
            return Response(
                {'info': 'Contents not available for this resource.'},
                status=status.HTTP_200_OK
            )
        else:
            if (settings.PAGE_PARAM in request.query_params) and \
                (resource_supports_pagination(r.resource_type)):

                paginator = resource_type.get_paginator()
                try:
                    results = paginator.paginate_queryset(contents, request)
                except NonIterableContentsException as ex:
                    # certain resources (e.g. JSON) can support pagination in
                    # certain contexts, such as is the JSON is essentially an 
                    # array. If the paginator raises this error, just return the
                    # entire contents we parsed before.
                    logging.info(f'Contents of resource ({resource_pk}) were'
                        ' not iterable. Returning all contents.'
                    )
                    return Response(contents)
                results = resource_type.to_json(results)
                return paginator.get_paginated_response(results)
            else:
                contents = resource_type.to_json(contents)
                return Response(contents)


class ResourceCreate(APIView):

    NOT_FOUND_MESSAGE = 'No workspace found by ID: {id}'

    def post(self, request, *args, **kwargs):

        logger.info(
            f'POSTing to create a new Resource with data={request.data}')
        try:
            data = request.data['data']
            workspace_pk = request.data['workspace']
            resource_type = request.data['resource_type']
        except KeyError as ex:
            return Response({str(ex): f'You must supply this required key.'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            resource_type_instance = get_resource_type_instance(resource_type)
        except KeyError as ex:
            return Response({str(ex): 'This resource type key is not valid.'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            workspace = Workspace.objects.get(id=workspace_pk, owner=request.user)
        except Workspace.DoesNotExist as ex:
            return Response(
                {'message': self.NOT_FOUND_MESSAGE.format(id=workspace_pk)}, 
                status=status.HTTP_404_NOT_FOUND)

        # now attempt to create the new file
        tmp_name = str(uuid.uuid4())
        tmp_path = f'{settings.TMP_DIR}/{tmp_name}'
    
        try:
            with open(tmp_path, 'w') as f:
                resource_type_instance.save_to_file(data, f)
        except Exception as ex:
            return Response(status = status.HTTP_400_BAD_REQUEST)

        with read_local_file(tmp_path, 'rb') as fin:
            fh = File(fin, tmp_name)
            resource = create_resource(
                request.user,
                file_handle=fh,
                name=tmp_name,
                workspace=workspace
            )
        file_format = retrieve_resource_class_standard_format(resource_type)
        initiate_resource_validation(resource, resource_type, file_format)
        if resource.resource_type == resource_type:
            return Response({'pk': resource.pk})
        else:
            logger.info('Resource type was not as expected- resource'
                       f' {resource.pk} failed validation.')
            resource.delete()
            return Response(status = status.HTTP_400_BAD_REQUEST)


class ResourcePreview(APIView):
    '''
    Returns a preview of the data underlying a Resource.

    Depending on the data, the format of the response may be different.
    Additionally, some Resource types do not support a preview.
    
    This returns a JSON-format representation of the data.

    This endpoint is only really sensible for certain types of 
    Resources, such as those in table format.  Other types, such as 
    sequence-based files do not have this functionality.
    '''

    def get(self, request, *args, **kwargs):
        user = request.user
        resource_pk = kwargs['pk']

        valid_request, r = check_resource_request(user, resource_pk)
        if not valid_request:
            # if the request was not valid, then `r` is a Response object.
            return r
        # requester can access, resource is active.  Go get contents
        try:
            resource_type = get_resource_type_instance(r.resource_type)
            contents = resource_type.get_contents(r, request.query_params, preview=True)
        except Exception as ex:
            return Response(
                {'error': 'Experienced an issue when preparing'
                          f' the resource preview: {ex}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )   
        if contents is None:
            return Response(
                {'info': 'Contents not available for this resource.'},
                status=status.HTTP_200_OK
            )
        else:
            contents = resource_type.to_json(contents)
            return Response(contents)


class AddBucketResourceView(APIView):
    '''
    This view is used to create a new user-associated resource given
    a path to a bucket-based file. 

    Use-cases for this endpoint are where we have example or public data files
    which we would like to attach to a particular user. The tutorial files are
    an example of this. Thus, the user does not have to download and then 
    subsequently upload to run through the tutorial example.
    '''

    BUCKET_PATH = 'bucket_path'
    RESOURCE_TYPE = 'resource_type'

    def post(self, request, *args, **kwargs):
        logger.info('POSTing to create a new resource from bucket-based data')

        try:
            src_path = request.data[self.BUCKET_PATH]
        except KeyError as ex:
            return Response({self.BUCKET_PATH: 'You must supply this required key.'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            resource_type = request.data[self.RESOURCE_TYPE]
        except KeyError as ex:
            resource_type = None

        try:
            file_format = request.data['file_format']
        except KeyError as ex:
            file_format = None

        try:
            r = default_storage.create_resource_from_interbucket_copy(request.user, src_path)
        except NotImplementedError:
            return Response({self.BUCKET_PATH: 'The storage system does not support this endpoint.'},
                status=status.HTTP_400_BAD_REQUEST
            )
        except FileNotFoundError:
            return Response({self.BUCKET_PATH: f'The path {src_path} could not be found.'},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception:
            #TODO: implement some catch for situations where the bucket
            # is not accessible, etc. 
            return Response({self.BUCKET_PATH: msg},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        # Even if the resource type or format were not set, we can 
        # call this function
        async_validate_resource.delay(r.pk, resource_type, file_format)

        resource_serializer = ResourceSerializer(r, context={'request': request})
        return Response(resource_serializer.data, status=status.HTTP_201_CREATED)


class ResourceContentTransform(ResourceContents):
    '''
    Endpoint for performing transforms on resource contents. Used in situations where frontend data
    transformations are not feasible.

    This class derives from ResourceContents so we can re-use the method for checking 
    the request validity. Since we are effectively returning a transformed view of the contents,
    this is reasonable.
    '''

    def get(self, request, *args, **kwargs):
        user = request.user
        resource_pk=kwargs['pk']
        valid_request, r = check_resource_request(user, resource_pk)
        if not valid_request:
            # if the request was not valid, then `r` is a Response object.
            return r
        else:
            query_params = request.query_params
            try:
                transform_fn = get_transformation_function(query_params['transform-name'])
                result = transform_fn(r, query_params)
                return Response(result)
            except KeyError as ex:
                return Response(
                    {'error': f'The request must contain the {ex} parameter.'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )   
            except Exception as ex:
                return Response({'error': str(ex)}, status=status.HTTP_400_BAD_REQUEST)