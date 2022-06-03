import logging
import json

from rest_framework.views import APIView
from rest_framework import permissions as framework_permissions
from rest_framework import status
from rest_framework.response import Response

from api.serializers.resource_types import ResourceTypeSerializer
from constants import DATABASE_RESOURCE_TYPES
from resource_types import RESOURCE_MAPPING


logger = logging.getLogger(__name__)

class ResourceTypeList(APIView):
    '''
    Lists available Resource types.

    Users can set their files to one of these types.

    Typically used for populating selections.
    '''
    
    permission_classes = [framework_permissions.AllowAny]
    serializer_class = ResourceTypeSerializer

    def get_serializer(self, *args, **kwargs):
        return self.serializer_class(*args, **kwargs)

    def get(self, request, *args, **kwargs):
        all_types = []
        for key, title in DATABASE_RESOURCE_TYPES:
            resource_type_class = RESOURCE_MAPPING[key]
            description =  resource_type_class.DESCRIPTION
            try:
                example = resource_type_class.EXAMPLE
            except AttributeError as ex:
                example = None
            all_types.append({
                'resource_type_key': key,
                'resource_type_title': title,
                'resource_type_description': description,
                'example': example
            })
        rs = self.get_serializer(all_types, many=True)
        return Response(rs.data)
