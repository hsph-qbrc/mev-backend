from rest_framework import serializers, exceptions

from api.models import WorkspaceExecutedOperation, Operation, OperationCategory

class OperationField(serializers.RelatedField):
    def to_representation(self, value):
        OpCategories = OperationCategory.objects.filter(operation=value)
        categories = list(set([x.category for x in OpCategories]))
        return {
            'operation_id': str(value.id),
            'operation_name': value.name,
            'categories': categories,
            'active': value.active
        }

class WorkspaceExecutedOperationSerializer(serializers.ModelSerializer):
    operation = OperationField(many=False, read_only=True)

    class Meta:
        model = WorkspaceExecutedOperation
        fields = [
            'id',
            'owner',
            'workspace', 
            'operation',
            'job_id',
            'job_name',
            'inputs',
            'outputs',
            'error_messages', 
            'status',
            'execution_start_datetime',
            'execution_stop_datetime',
            'job_failed',
            'is_finalizing',
            'mode'
        ]