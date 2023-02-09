from .api_root import ApiRoot
from .user_views import UserList, \
    UserDetail, \
    UserRegisterView, \
    PasswordResetView, \
    PasswordResetConfirmView, \
    UserActivateView, \
    ResendActivationView, \
    PasswordChangeView
from .social_views import GoogleOauth2View
from .workspace_views import WorkspaceList, WorkspaceDetail
from .resource_views import ResourceList, \
    ResourceDetail, \
    ResourceContents, \
    AddBucketResourceView, \
    ResourceContentTransform
from .resource_download import ResourceDownload, \
    ResourceDownloadUrl
from .operation_resource_views import OperationResourceList, \
    OperationResourceFieldList
from .workspace_resource_views import WorkspaceResourceList, \
    WorkspaceResourceAdd, \
    WorkspaceResourceRemove
from .workspace_metadata_views import WorkspaceMetadataObservationsView, \
    WorkspaceMetadataFeaturesView
from .metadata_operations_views import MetadataIntersectView, \
    MetadataUnionView, \
    MetadataSetDifferenceView
from .workspace_tree_views import WorkspaceTreeView, WorkspaceTreeSave
from .resource_upload_views import ResourceUploadView, \
    DropboxUpload
from .resource_metadata import ResourceMetadataView, \
    ResourceMetadataObservationsView, \
    ResourceMetadataFeaturesView, \
    ResourceMetadataParentOperationView
from .token_views import TokenObtainView, RefreshTokenView
from .resource_type_views import ResourceTypeList
from .operation_views import OperationList, \
    OperationDetail, \
    OperationCreate, \
    OperationUpdate
from .executed_operation_views import OperationRun, \
    ExecutedOperationCheck, \
    ExecutedOperationList, \
    NonWorkspaceExecutedOperationList, \
    WorkspaceExecutedOperationList
from .operation_category_views import OperationCategoryList, \
    OperationCategoryDetail, \
    OperationCategoryAdd
from .public_dataset import PublicDatasetList, \
    PublicDatasetDetails, \
    PublicDatasetQuery, \
    PublicDatasetCreate
from .feedback_views import SubmitFeedbackView
from .globus_views import GlobusInitiate, \
    GlobusUploadView, \
    GlobusTransferList
from .random import RandomView

def sentry_debug(request):
    '''
    A function guaranteed to raise an exception to
    test that we have configured Sentry correctly
    and it is receiving exceptions.
    '''
    return 1/0
