import json
import unittest.mock as mock
import uuid
import tempfile

from exceptions import AttributeValueError, \
    DataStructureValidationException, \
    StringIdentifierException, \
    StorageException, \
    OutputConversionException

from data_structures.observation_set import ObservationSet
from data_structures.feature_set import FeatureSet
from data_structures.operation_output import OperationOutput

from api.models import Resource
from api.converters.basic_attributes import StringConverter, \
    IntegerConverter, \
    StringListConverter, \
    UnrestrictedStringConverter, \
    UnrestrictedStringListConverter, \
    StringListToCsvConverter, \
    UnrestrictedStringListToCsvConverter, \
    BooleanAsIntegerConverter, \
    NormalizingListToCsvConverter, \
    NormalizingStringConverter

from api.converters.data_resource import \
    BaseResourceConverter, \
    VariableDataResourceMixin, \
    MultipleDataResourceMixin, \
    LocalResourceMixin, \
    LocalDockerSingleDataResourceConverter, \
    LocalDockerSingleVariableDataResourceConverter, \
    LocalDockerSingleDataResourceWithTypeConverter, \
    LocalDockerMultipleDataResourceConverter, \
    LocalDockerMultipleVariableDataResourceConverter, \
    LocalDockerCsvResourceConverter, \
    LocalDockerSpaceDelimResourceConverter, \
    RemoteNextflowSingleDataResourceConverter, \
    RemoteResourceMixin, \
    RemoteNextflowSingleVariableDataResourceConverter, \
    ECSSingleDataResourceConverter, \
    ECSSingleVariableDataResourceConverter
    # RemoteNextflowMultipleDataResourceConverter, \

from api.converters.element_set import ObservationSetCsvConverter, \
    FeatureSetCsvConverter, \
    ObservationSetListConverter, \
    FeatureSetListConverter

from api.converters.json_converters import JsonConverter

from api.tests.base import BaseAPITestCase


class TestBasicAttributeConverter(BaseAPITestCase):

    def test_basic_attributes(self):
        s = StringConverter()
        v = s.convert_input('abc', '', '')
        self.assertEqual(v, 'abc')
        v = s.convert_output('','','', 'abc')
        self.assertEqual(v, 'abc')

        v = s.convert_input('ab c', '', '')
        self.assertEqual(v, 'ab_c')
        v = s.convert_output('','','', 'ab c')
        self.assertEqual(v, 'ab_c')

        with self.assertRaises(AttributeValueError):
            v = s.convert_input('ab?c', '', '')

        with self.assertRaises(AttributeValueError):
            v = s.convert_output('','','', 'ab?c')

        s = UnrestrictedStringConverter()
        v = s.convert_input('abc', '', '')
        self.assertEqual(v, 'abc')
        v = s.convert_output('','','', 'abc')
        self.assertEqual(v, 'abc')

        v = s.convert_input('ab c', '', '')
        self.assertEqual(v, 'ab c')
        v = s.convert_output('','','', 'ab c')
        self.assertEqual(v, 'ab c')

        v = s.convert_input('ab?c', '', '')
        self.assertEqual(v, 'ab?c')
        v = s.convert_output('','','', 'ab?c')
        self.assertEqual(v, 'ab?c')

        ic = IntegerConverter()
        i = ic.convert_input( 2, '', '')
        self.assertEqual(i,2)
        i = ic.convert_output('','','', 2)
        self.assertEqual(i,2)

        with self.assertRaises(AttributeValueError):
            ic.convert_input('1', '', '')
        with self.assertRaises(AttributeValueError):
            ic.convert_input(1.2, '', '')
        with self.assertRaises(AttributeValueError):
            ic.convert_output('','','','1')
        with self.assertRaises(AttributeValueError):
            ic.convert_output('','','',1.2)

        with self.assertRaises(AttributeValueError):
            ic.convert_input('a', '', '')

        with self.assertRaises(AttributeValueError):
            ic.convert_output('', '', '', 'a')

        s = StringListConverter()
        v = s.convert_input( ['ab','c d'], '', '')
        self.assertCountEqual(['ab','c_d'], v)
        v = s.convert_output('','','', ['ab','c d'])
        self.assertCountEqual(['ab','c_d'], v)

        with self.assertRaises(DataStructureValidationException):
            v = s.convert_input( 2, '', '')

        with self.assertRaises(DataStructureValidationException):
            s.convert_output('', '', '', 2)

        v = s.convert_input( ['1','2'], '', '')
        self.assertCountEqual(['1','2'], v)

        v = s.convert_output('', '', '', ['1','2'])
        self.assertCountEqual(['1','2'], v)

        s = UnrestrictedStringListConverter()
        v = s.convert_input( ['ab','c d'], '', '')
        self.assertCountEqual(['ab','c d'], v)
        v = s.convert_output('','','', ['ab','c d'])
        self.assertCountEqual(['ab','c d'], v)

        c = StringListToCsvConverter()
        v = c.convert_input( ['aaa','bbb','ccc'], '', '')
        self.assertEqual(v, 'aaa,bbb,ccc')

        c = StringListToCsvConverter()
        v = c.convert_input( ['a b','c d'], '', '')
        self.assertEqual(v, 'a_b,c_d')

        c = StringListToCsvConverter()
        with self.assertRaises(AttributeValueError):
            v = c.convert_input( ['a?b','c d'], '', '')

        c = UnrestrictedStringListToCsvConverter()
        v = c.convert_input(['aaa','bbb','ccc'], '', '')
        self.assertEqual(v, 'aaa,bbb,ccc')

        c = UnrestrictedStringListToCsvConverter()
        v = c.convert_input( ['a b','c d'], '', '')
        self.assertEqual(v, 'a b,c d')

        c = UnrestrictedStringListToCsvConverter()
        v = c.convert_input( ['a?b','c d'], '', '')
        self.assertEqual(v, 'a?b,c d')
        
        c = NormalizingListToCsvConverter()
        v = c.convert_input( ['a b', 'c  d'], '', '')
        self.assertEqual(v, 'a_b,c__d')

        c = NormalizingListToCsvConverter()
        with self.assertRaises(StringIdentifierException):
            v = c.convert_input( ['a b', 'c ? d'], '', '')

        c = NormalizingStringConverter()
        v = c.convert_input( 'a b.tsv', '', '')
        self.assertEqual(v, 'a_b.tsv')

        c = NormalizingStringConverter()
        with self.assertRaises(StringIdentifierException):
            v = c.convert_input( 'c ? d', '', '')

        c = BooleanAsIntegerConverter()
        x = c.convert_input( 1, '/tmp', '')
        self.assertEqual(x, 1)

        x = c.convert_input( True, '/tmp', '')
        self.assertEqual(x, 1)

        x = c.convert_input( 'true', '/tmp', '')
        self.assertEqual(x, 1)

        with self.assertRaises(AttributeValueError):
            x = c.convert_input( '1', '/tmp', '')

        # check the false'y vals:
        x = c.convert_input( 0, '/tmp', '')
        self.assertEqual(x, 0)

        x = c.convert_input( False, '/tmp', '')
        self.assertEqual(x, 0)

        x = c.convert_input( 'false', '/tmp', '')
        self.assertEqual(x, 0)

        with self.assertRaises(AttributeValueError):
            x = c.convert_input( '0', '/tmp', '')


class TestElementSetConverter(BaseAPITestCase):

    def test_observation_set_csv_converter(self):
        obs_set = ObservationSet(
            {'elements': [
                {
                    'id': 'foo'
                },
                {
                    'id': 'bar'
                }
            ]}
        )
        d = obs_set.to_simple_dict()
        c = ObservationSetCsvConverter()  
        # order doesn't matter, so need to check both orders: 
        converted_input = c.convert_input(d, '', '') 
        self.assertTrue(
            ('foo,bar' == converted_input)
            |
            ('bar,foo' == converted_input)
        )

    def test_feature_set_csv_converter(self):
        f_set = FeatureSet(
            {'elements': [
                {
                    'id': 'foo'
                },
                {
                    'id': 'bar'
                }
            ]}
        )
        d = f_set.to_simple_dict()
        c = FeatureSetCsvConverter()  
        # order doesn't matter, so need to check both orders:      
        converted_input = c.convert_input(d, '', '') 
        self.assertTrue(
            ('foo,bar' == converted_input)
            |
            ('bar,foo' == converted_input)
        )

    def test_observation_set_list_converter(self):
        '''
        Tests that we get properly formatted JSON-compatible
        arrays (of strings in this case). Used when we need to
        supply a WDL job with a list of relevant samples as an
        array of strings, for instance.
        '''
        obs_set = ObservationSet(
            {'elements': [
                {
                    'id': 'foo'
                },
                {
                    'id': 'bar'
                }
            ]}
        )
        d = obs_set.to_simple_dict()
        c = ObservationSetListConverter()  
        # order doesn't matter, so need to check both orders: 
        converted_input = c.convert_input(d, '', '') 
        self.assertCountEqual(['foo','bar'], converted_input)

    def test_feature_set_list_converter(self):
        '''
        Tests that we get properly formatted JSON-compatible
        arrays (of strings in this case). Used when we need to
        supply a WDL job with a list of relevant samples as an
        array of strings, for instance.
        '''
        f_set = FeatureSet(
            {'elements': [
                {
                    'id': 'foo'
                },
                {
                    'id': 'bar'
                }
            ]}
        )
        d = f_set.to_simple_dict()
        c = FeatureSetListConverter()  
        # order doesn't matter, so need to check both orders: 
        converted_input = c.convert_input(d, '', '') 
        self.assertCountEqual(['foo','bar'], converted_input)

class TestBaseResourceConverter(BaseAPITestCase):

    def test_handle_storage_failure(self):
        c = BaseResourceConverter()

        # output was not required:
        c._handle_storage_failure(False)

        # when an output is required but there was a storage
        # failure, need to raise an exception
        with self.assertRaises(OutputConversionException):
            c._handle_storage_failure(True)

    @mock.patch('api.converters.data_resource.delete_resource_by_pk')
    def test_handles_invalid_resource_type(self, mock_delete_resource_by_pk):
        mock_resource = mock.MagicMock()
        mock_pk = 'abc123'
        mock_resource.pk = mock_pk
        c = BaseResourceConverter()
        c._handle_invalid_resource_type(mock_resource)
        mock_delete_resource_by_pk.assert_called_once_with(mock_pk)

    def test_creates_output_filename(self):
        c = BaseResourceConverter()
        mock_job_name = 'foo'
        mock_path = '/some/path/bar.txt'

        result = c._create_output_filename(mock_path, '')
        self.assertTrue(result == 'bar.txt')

        result = c._create_output_filename(mock_path, mock_job_name)
        self.assertTrue(result == f'{mock_job_name}.bar.txt')

    @mock.patch('api.converters.data_resource.alert_admins')
    @mock.patch('api.converters.data_resource.ResourceMetadata')
    @mock.patch('api.converters.data_resource.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.data_resource.initiate_resource_validation')
    def test_attempt_resource_addition_case1(self, 
        mock_initiate_resource_validation, 
        mock_retrieve_resource_class_standard_format,
        mock_ResourceMetadata,
        mock_alert_admins):
        '''
        Test the successful path through this method
        '''

        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        mock_path = '/some/path/file.txt'
        mock_resource = mock.MagicMock()
        mock_pk = 'abc123'
        mock_resource.pk = mock_pk
        mock_resource_type = 'mock_rt'

        c = BaseResourceConverter()
        mock_create_output_filename = mock.MagicMock()
        mock_name = 'some.name.txt'
        mock_create_output_filename.return_value = mock_name
        c._create_output_filename = mock_create_output_filename

        # test the successful path through the method
        mock_format = 'TSV'
        mock_retrieve_resource_class_standard_format.return_value = mock_format
        mock_create_resource = mock.MagicMock()
        mock_create_resource.return_value = mock_resource
        c._create_resource = mock_create_resource
        result = c._attempt_resource_addition(mock_executed_op,
            mock_workspace, mock_path, mock_resource_type, True)
        self.assertEqual(result, mock_pk)
        mock_create_resource.assert_called_once_with(mock_executed_op,
            mock_workspace, mock_path, mock_name)    
        mock_retrieve_resource_class_standard_format.assert_called_once_with(mock_resource_type)
        mock_initiate_resource_validation.assert_called_once_with(mock_resource,
            mock_resource_type, mock_format)
        self.assertTrue(mock_resource.is_active)
        mock_resource.save.assert_called()

    @mock.patch('api.converters.data_resource.alert_admins')
    @mock.patch('api.converters.data_resource.ResourceMetadata')
    @mock.patch('api.converters.data_resource.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.data_resource.initiate_resource_validation')
    def test_attempt_resource_addition_case2(self, 
        mock_initiate_resource_validation, 
        mock_retrieve_resource_class_standard_format,
        mock_ResourceMetadata,
        mock_alert_admins):
        '''
        Test the case where _create_resource raises a StorageException
        error. Then we test that the correct actions are performed 
        depending on the execution
        '''

        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        mock_path = '/some/path/file.txt'
        mock_resource_type = 'mock_rt'

        c = BaseResourceConverter()
        mock_create_output_filename = mock.MagicMock()
        mock_name = 'some.name.txt'
        mock_create_output_filename.return_value = mock_name
        c._create_output_filename = mock_create_output_filename

        mock_create_resource = mock.MagicMock()
        mock_create_resource.side_effect = StorageException('!!!')
        c._create_resource = mock_create_resource

        # note that we don't mock out the _handle_storage_failure
        # method for the moment, since it's such a simple method

        # here output is NOT required so we simply return None and
        # don't raise any exceptions.
        result = c._attempt_resource_addition(mock_executed_op,
            mock_workspace, mock_path, mock_resource_type, False)
        self.assertIsNone(result)
        mock_initiate_resource_validation.assert_not_called()
        mock_retrieve_resource_class_standard_format.assert_not_called()
        mock_alert_admins.assert_not_called()

        # here an exception is raised since we specified that the
        # output WAS required.
        mock_initiate_resource_validation.reset_mock()
        mock_retrieve_resource_class_standard_format.reset_mock()
        mock_alert_admins.reset_mock()
        with self.assertRaises(OutputConversionException):
            c._attempt_resource_addition(mock_executed_op,
                mock_workspace, mock_path, mock_resource_type, True)
        mock_initiate_resource_validation.assert_not_called()
        mock_retrieve_resource_class_standard_format.assert_not_called()
        mock_alert_admins.assert_not_called()

    @mock.patch('api.converters.data_resource.alert_admins')
    @mock.patch('api.converters.data_resource.ResourceMetadata')
    @mock.patch('api.converters.data_resource.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.data_resource.initiate_resource_validation')
    def test_attempt_resource_addition_case3(self, 
        mock_initiate_resource_validation, 
        mock_retrieve_resource_class_standard_format,
        mock_ResourceMetadata,
        mock_alert_admins):
        '''
        Test the case where _create_resource raises a generic exception.
        Then we test that the correct actions are performed 
        depending on the execution
        '''
        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        mock_path = '/some/path/file.txt'
        mock_resource_type = 'mock_rt'

        c = BaseResourceConverter()
        mock_create_output_filename = mock.MagicMock()
        mock_name = 'some.name.txt'
        mock_create_output_filename.return_value = mock_name
        c._create_output_filename = mock_create_output_filename

        mock_create_resource = mock.MagicMock()
        mock_create_resource.side_effect = Exception('!!!')
        c._create_resource = mock_create_resource

        # here output is NOT required so we simply return None and
        # don't raise any exceptions. However, we DO call the admins
        # since an unexpected exception was raised
        result = c._attempt_resource_addition(mock_executed_op,
            mock_workspace, mock_path, mock_resource_type, False)
        self.assertIsNone(result)
        mock_initiate_resource_validation.assert_not_called()
        mock_retrieve_resource_class_standard_format.assert_not_called()
        mock_alert_admins.assert_called()

        # here an exception is raised since we specified that the
        # output WAS required. Admins are also notified
        mock_initiate_resource_validation.reset_mock()
        mock_retrieve_resource_class_standard_format.reset_mock()
        mock_alert_admins.reset_mock()
        with self.assertRaises(OutputConversionException):
            c._attempt_resource_addition(mock_executed_op,
                mock_workspace, mock_path, mock_resource_type, True)
        mock_initiate_resource_validation.assert_not_called()
        mock_retrieve_resource_class_standard_format.assert_not_called()
        mock_alert_admins.assert_called()

    @mock.patch('api.converters.data_resource.alert_admins')
    @mock.patch('api.converters.data_resource.ResourceMetadata')
    @mock.patch('api.converters.data_resource.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.data_resource.initiate_resource_validation')
    def test_attempt_resource_addition_case4(self, 
        mock_initiate_resource_validation, 
        mock_retrieve_resource_class_standard_format,
        mock_ResourceMetadata,
        mock_alert_admins):
        '''
        Test that everything works as far as finding the file, but
        the format is invalid
        '''

        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        mock_path = '/some/path/file.txt'
        mock_resource = mock.MagicMock()
        mock_pk = 'abc123'
        mock_resource.pk = mock_pk
        mock_resource_type = 'mock_rt'

        c = BaseResourceConverter()
        mock_create_output_filename = mock.MagicMock()
        mock_name = 'some.name.txt'
        mock_create_output_filename.return_value = mock_name
        c._create_output_filename = mock_create_output_filename

        mock_format = 'TSV'
        mock_retrieve_resource_class_standard_format.return_value = mock_format
        mock_create_resource = mock.MagicMock()
        mock_create_resource.return_value = mock_resource
        c._create_resource = mock_create_resource

        mock_initiate_resource_validation.side_effect = Exception('!!!')

        mock_handle_invalid_resource_type = mock.MagicMock()
        c._handle_invalid_resource_type = mock_handle_invalid_resource_type

        with self.assertRaises(OutputConversionException):
            c._attempt_resource_addition(mock_executed_op,
                mock_workspace, mock_path, mock_resource_type, True)
        mock_alert_admins.assert_called()
        mock_create_resource.assert_called_once_with(mock_executed_op,
            mock_workspace, mock_path, mock_name)    
        mock_retrieve_resource_class_standard_format.assert_called_once_with(mock_resource_type)
        mock_initiate_resource_validation.assert_called_once_with(mock_resource,
            mock_resource_type, mock_format)

    def test_convert_resource_output(self):
        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        mock_path = '/some/path/file.txt'
        mock_pk = 'abc123'
        mock_resource_type = 'mock_rt'

        mock_get_output_path_and_resource_type = mock.MagicMock()
        mock_get_output_path_and_resource_type.return_value = (mock_path, mock_resource_type)

        mock_attempt_resource_addition = mock.MagicMock()
        mock_attempt_resource_addition.return_value = mock_pk

        c = BaseResourceConverter()
        c._get_output_path_and_resource_type = mock_get_output_path_and_resource_type
        c._attempt_resource_addition = mock_attempt_resource_addition
        output_definition = mock.MagicMock()
        output_definition.required = True
        output_definition.spec = 'xyz'
        mock_output_val = 'output_val'
        result = c._convert_resource_output(mock_executed_op,
            mock_workspace, output_definition, mock_output_val)
        self.assertEquals(result, mock_pk)
        mock_get_output_path_and_resource_type.assert_called_once_with(mock_output_val, 'xyz')
        mock_attempt_resource_addition.assert_called_once_with(mock_executed_op,
            mock_workspace, mock_path, mock_resource_type, True)

        # mock the situation where the resource addition has an issue:
        mock_attempt_resource_addition.reset_mock()
        mock_attempt_resource_addition.side_effect = Exception('ACK!!!')
        with self.assertRaisesRegex(OutputConversionException, 'ACK!!!'):
            c._convert_resource_output(mock_executed_op,
                mock_workspace, output_definition, mock_output_val)


class TestVariableDataResourceMixin(BaseAPITestCase):

    def test_unexpected_output_val(self):
        '''
        Variable data resources arise when we have a process/tool
        that can take multiple input resource types. For example,
        a tool that will subset the rows of a matrix. In that case,
        we don't want different tools for each potential resource type
        (e.g. matrix, integer matrix, feature table).
        Hence, these tools will preserve the resource type of the inputs.
        For example, if the input was of type MTX, then the output should
        also be a MTX. The way we specify this is that the output field
        of the api.models.ExecutedOperation has a dict format giving the
        path to the output file and the resource type. If we receive
        an output that is different, we need to catch that and inform
        admins. Luckily, such an error is a problem with the tool itself
        and is NOT an end-user error.
        '''
        v = VariableDataResourceMixin()
        mock_output_spec = {}
        with self.assertRaises(OutputConversionException):
            v._get_output_path_and_resource_type('some/path', mock_output_spec)

    def test_missing_keys(self):
        '''
        In the value dict passed to this method, we need both the `path`
        and `resource_type` keys.
        '''
        v = VariableDataResourceMixin()
        mock_output_spec = mock.MagicMock()
        mock_output_spec.value.resource_types = []
        with self.assertRaisesRegex(OutputConversionException, 'resource_type'):
            v._get_output_path_and_resource_type({'path': '/some/path'}, mock_output_spec)

        with self.assertRaisesRegex(OutputConversionException, 'path'):
            v._get_output_path_and_resource_type({'resource_type': 'MTX'}, mock_output_spec)

    def test_incompatible_resource_types(self):
        '''
        The requested and specified resource types need to match
        '''
        v = VariableDataResourceMixin()
        mock_output_spec = mock.MagicMock()
        mock_output_spec.value.resource_types = ['AAA', 'BBB']
        with self.assertRaisesRegex(OutputConversionException, 'not consistent'):
            v._get_output_path_and_resource_type(
                {'path': '/some/path', 'resource_type': 'CCC'},
                mock_output_spec)

    def test_returns_as_expected(self):
        '''
        Tests the expected path through the method
        '''
        v = VariableDataResourceMixin()
        mock_output_spec = mock.MagicMock()
        mock_output_spec.value.resource_types = ['AAA', 'BBB']
        result = v._get_output_path_and_resource_type(
                {'path': '/some/path', 'resource_type': 'BBB'},
                mock_output_spec)
        self.assertEquals(result[0], '/some/path')
        self.assertEquals(result[1], 'BBB')


class TestMultipleDataResourceMixin(BaseAPITestCase):

    def test_convert_input_method_case1(self):
        '''
        Tests the expected path through when provided a list
        '''
        m = MultipleDataResourceMixin()
        mock_convert_resource_input = mock.MagicMock()
        mock_convert_resource_input.side_effect = ['a','b']
        m._convert_resource_input = mock_convert_resource_input
        mock_staging_dir = '/some/dir'
        result = m._convert_input(['a','b'], mock_staging_dir)
        self.assertCountEqual(result, ['a','b'])
        mock_convert_resource_input.assert_has_calls([
            mock.call('a', mock_staging_dir),
            mock.call('b', mock_staging_dir)
        ])

    def test_convert_input_method_case2(self):
        '''
        Tests the expected path through when provided a str
        '''
        m = MultipleDataResourceMixin()
        mock_convert_resource_input = mock.MagicMock()
        mock_convert_resource_input.return_value = 'A'
        m._convert_resource_input = mock_convert_resource_input
        mock_staging_dir = '/some/dir'
        result = m._convert_input('a', mock_staging_dir)
        self.assertCountEqual(result, ['A'])
        mock_convert_resource_input.assert_has_calls([
            mock.call('a', mock_staging_dir),
        ])

    def test_convert_output_method_case1(self):
        '''
        Test the expected path through the method
        '''
        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": True,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)
        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        output_val = ['a','b']
        m = MultipleDataResourceMixin()
        mock_convert_resource_output = mock.MagicMock()
        mock_convert_resource_output.side_effect = ['A', 'B']
        m._convert_resource_output = mock_convert_resource_output

        mock_cleanup = mock.MagicMock()
        m._cleanup_other_outputs = mock_cleanup

        result = m._convert_output(mock_executed_op, mock_workspace, o, output_val)
        self.assertCountEqual(result, ['A', 'B'])
        mock_convert_resource_output.assert_has_calls([
            mock.call(mock_executed_op, mock_workspace, o, 'a'),
            mock.call(mock_executed_op, mock_workspace, o, 'b')
        ])
        mock_cleanup.assert_not_called()

    def test_convert_output_method_case2(self):
        '''
        Test that we raise an exception if this method is called when 
        a single output was expected
        '''
        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": False,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)
        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        output_val = ['a','b']
        m = MultipleDataResourceMixin()
        with self.assertRaisesRegex(OutputConversionException, 'multiple'):
            m._convert_output(mock_executed_op, mock_workspace, o, output_val)
        
    def test_convert_output_method_case3(self):
        '''
        Test that we raise an exception if this method is called with 
        a non-list value
        '''
        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": True,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)
        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        output_val = 'a'
        m = MultipleDataResourceMixin()
        with self.assertRaisesRegex(OutputConversionException, 'expect a list'):
            m._convert_output(mock_executed_op, mock_workspace, o, output_val)

    def test_cleans_up_is_encounters_failure(self):
        '''
        We don't want to leave jobs in an ambiguous/incomplete
        state if one of the multiple resources fails to convert.
        If we encounter a failure, we should clean the other outputs
        and raise an exception
        '''
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": True,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)
        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        output_val = ['a','b', 'c']
        m = MultipleDataResourceMixin()
        mock_convert_resource_output = mock.MagicMock()
        mock_convert_resource_output.side_effect = ['A', OutputConversionException('!!!')]
        m._convert_resource_output = mock_convert_resource_output

        mock_cleanup = mock.MagicMock()
        m._cleanup_other_outputs = mock_cleanup

        with self.assertRaisesRegex(OutputConversionException, '!!!'):
            m._convert_output(mock_executed_op, mock_workspace, o, output_val)
        # only needs to clean up the first one:
        mock_cleanup.assert_called_once_with(['A'])
        mock_convert_resource_output.assert_has_calls([
            mock.call(mock_executed_op, mock_workspace, o, 'a'),
            mock.call(mock_executed_op, mock_workspace, o, 'b')
        ])

class TestLocalResourceMixin(BaseAPITestCase):

    @mock.patch('api.converters.data_resource.create_resource')
    @mock.patch('api.converters.data_resource.File')
    def test_create_resource(self, mock_File_class, mock_create_resource):

        mock_resource = mock.MagicMock()
        mock_create_resource.return_value = mock_resource

        mock_executed_op = mock.MagicMock()
        mock_owner = mock.MagicMock()
        mock_executed_op.owner = mock_owner
        mock_workspace = mock.MagicMock()
        mock_path = '/some/path/file.txt'
        mock_name = 'abc'
        mock_file_obj = mock.MagicMock()
        mock_File_class.return_value = mock_file_obj

        c = LocalResourceMixin()
        with mock.patch("builtins.open", mock.mock_open(read_data="data")) as mock_file:
            r = c._create_resource(mock_executed_op, mock_workspace, mock_path, mock_name)
            self.assertEqual(r, mock_resource)
            mock_create_resource.assert_called_once_with(
                mock_owner,
                file_handle=mock_file_obj,
                name=mock_name,
                workspace=mock_workspace
            )

class TestRemoteResourceMixin(BaseAPITestCase):

    @mock.patch('api.converters.data_resource.default_storage')
    def test_create_workspace_resource(self, mock_default_storage):
        
        mock_resource = mock.MagicMock()
        mock_create_resource_from_interbucket_copy = mock.MagicMock()
        mock_create_resource_from_interbucket_copy.return_value = mock_resource
        mock_default_storage.create_resource_from_interbucket_copy = mock_create_resource_from_interbucket_copy

        mock_executed_op = mock.MagicMock()
        mock_owner = mock.MagicMock()
        mock_executed_op.owner = mock_owner
        mock_workspace = mock.MagicMock()
        mock_path = '/some/path/file.txt'
        mock_name = 'abc'

        c = RemoteResourceMixin()
        r = c._create_resource(mock_executed_op, mock_workspace, mock_path, mock_name)
        self.assertEqual(r, mock_resource)
        mock_create_resource_from_interbucket_copy.assert_called_once_with(
            mock_owner,
            mock_path
        )
        mock_resource.workspaces.add.assert_called_once_with(mock_workspace)
        self.assertTrue(mock_resource.name == mock_name)

        mock_create_resource_from_interbucket_copy.reset_mock()
        mock_create_resource_from_interbucket_copy.side_effect = Exception('!!!') 
        with self.assertRaises(Exception):
            c._create_resource(mock_executed_op, mock_workspace, mock_path, mock_name)

    @mock.patch('api.converters.data_resource.default_storage')
    def test_create_non_workspace_resource(self, mock_default_storage):
        
        mock_resource = mock.MagicMock()
        mock_create_resource_from_interbucket_copy = mock.MagicMock()
        mock_create_resource_from_interbucket_copy.return_value = mock_resource
        mock_default_storage.create_resource_from_interbucket_copy = mock_create_resource_from_interbucket_copy

        mock_executed_op = mock.MagicMock()
        mock_owner = mock.MagicMock()
        mock_executed_op.owner = mock_owner
        mock_path = '/some/path/file.txt'
        mock_name = 'abc'

        c = RemoteResourceMixin()
        r = c._create_resource(mock_executed_op, None, mock_path, mock_name)
        self.assertEqual(r, mock_resource)
        mock_create_resource_from_interbucket_copy.assert_called_once_with(
            mock_owner,
            mock_path
        )
        mock_resource.workspaces.add.assert_not_called()


class TestDataResourceConverter(BaseAPITestCase):


    @mock.patch('api.converters.data_resource.ResourceMetadata')
    @mock.patch('api.converters.data_resource.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.data_resource.initiate_resource_validation')
    def test_single_local_output_converter(self, 
        mock_initiate_resource_validation,
        mock_retrieve_resource_class_standard_format,
        mock_resource_metadata_class):
        '''
        Tests the conversion of a single output for a local 
        runner. Mocks out the actual resource creation and 
        some methods, but asserts that they are called with the
        expected args.

        This test is for a DataResource which has a fixed
        resource_type defined by the output spec.
        '''
        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": False,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)

        mock_retrieve_resource_class_standard_format.return_value = 'TSV'

        # note that it's arranged in a list so we can easily
        # add more implementations as we wish
        c1 = LocalDockerSingleDataResourceConverter()
        for c in [c1,]:

            mock_retrieve_resource_class_standard_format.reset_mock()
            mock_initiate_resource_validation.reset_mock()

            # mock out the actual creation of the Resource
            resource_pk = uuid.uuid4()
            mock_create_resource = mock.MagicMock()
            mock_resource = mock.MagicMock()
            mock_resource.pk = resource_pk
            mock_create_resource.return_value = mock_resource
            mock_create_output_filename = mock.MagicMock()
            mock_name = 'foo'
            mock_create_output_filename.return_value = mock_name
            mock_executed_op = mock.MagicMock()
            mock_executed_op.job_name = 'myjob'
            mock_workspace = mock.MagicMock()
            c._create_resource = mock_create_resource
            c._create_output_filename = mock_create_output_filename

            # now call the converter
            mock_path = '/some/path'
            u = c.convert_output(mock_executed_op, mock_workspace, o, mock_path)
            self.assertEqual(u, str(resource_pk))
            c._create_resource.assert_called_once_with(
                mock_executed_op,
                mock_workspace,
                mock_path,
                mock_name
            )
            mock_retrieve_resource_class_standard_format.assert_called_once_with('MTX')
            mock_initiate_resource_validation.assert_called_once_with(
                mock_resource,
                'MTX',
                'TSV'
            )
            self.assertTrue(mock_resource.is_active)
            mock_resource.save.assert_called()

    @mock.patch('api.converters.data_resource.ResourceMetadata')
    @mock.patch('api.converters.data_resource.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.data_resource.initiate_resource_validation')
    def test_single_local_output_variable_data_resource_converter(self, 
        mock_initiate_resource_validation,
        mock_retrieve_resource_class_standard_format,
        mock_resource_metadata_class):
        '''
        Tests the conversion of a single output for a local 
        runner. Mocks out the actual resource creation and 
        some methods, but asserts that they are called with the
        expected args.

        This tests for VariableDataResource types which can have
        multiple potential output resource_types
        '''
        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "VariableDataResource",
                "many": False,
                "resource_types": ["I_MTX","MTX"]
            }
        }
        o = OperationOutput(d)

        mock_retrieve_resource_class_standard_format.return_value = 'TSV'

        c1 = LocalDockerSingleVariableDataResourceConverter()
        for c in [c1,]:

            mock_retrieve_resource_class_standard_format.reset_mock()
            mock_initiate_resource_validation.reset_mock()

            # mock out the actual creation of the Resource
            resource_pk = uuid.uuid4()
            mock_create_resource = mock.MagicMock()
            mock_resource = mock.MagicMock()
            mock_resource.pk = resource_pk
            mock_create_resource.return_value = mock_resource
            mock_create_output_filename = mock.MagicMock()
            mock_name = 'foo'
            mock_create_output_filename.return_value = mock_name
            mock_executed_op = mock.MagicMock()
            mock_executed_op.job_name = 'myjob'
            mock_workspace = mock.MagicMock()
            c._create_resource = mock_create_resource
            c._create_output_filename = mock_create_output_filename

            # now call the converter
            mock_path = '/some/path'
            operation_output_payload = {
                'path': mock_path,
                'resource_type': "I_MTX"
            }
            u = c.convert_output(mock_executed_op, mock_workspace, o, operation_output_payload)
            self.assertEqual(u, str(resource_pk))
            c._create_resource.assert_called_once_with(
                mock_executed_op,
                mock_workspace,
                mock_path,
                mock_name
            )
            mock_retrieve_resource_class_standard_format.assert_called_once_with('I_MTX')
            mock_initiate_resource_validation.assert_called_once_with(
                mock_resource,
                'I_MTX',
                'TSV'
            )
            self.assertTrue(mock_resource.is_active)
            mock_resource.save.assert_called()

            # try with an invalid resource type
            mock_create_resource.reset_mock()
            mock_retrieve_resource_class_standard_format.reset_mock()
            mock_initiate_resource_validation.reset_mock()
            mock_resource.reset_mock()
            operation_output_payload = {
                'path': mock_path,
                'resource_type': "FT"
            }
            with self.assertRaisesRegex(OutputConversionException, 'not consistent'):
                u = c.convert_output(mock_executed_op, mock_workspace, o, operation_output_payload)

            c._create_resource.assert_not_called()
            mock_retrieve_resource_class_standard_format.assert_not_called()
            mock_initiate_resource_validation.assert_not_called()

    @mock.patch('api.converters.data_resource.ResourceMetadata')
    @mock.patch('api.converters.data_resource.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.data_resource.initiate_resource_validation')
    def test_single_local_output_converter_failure(self, 
        mock_initiate_resource_validation,
        mock_retrieve_resource_class_standard_format,
        mock_resource_metadata_class):
        '''
        Tests the conversion of a single output for a local 
        runner. Here, we mock there being an issue
        '''

        mock_retrieve_resource_class_standard_format.return_value = 'TSV'
        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": False,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)

        c1 = LocalDockerSingleDataResourceConverter()
        for c in [c1,]:

            # mock out the actual creation of the Resource
            mock_create_resource = mock.MagicMock()
            mock_create_resource.side_effect = StorageException('!!')

            mock_create_output_filename = mock.MagicMock()
            mock_name = 'foo'
            mock_create_output_filename.return_value = mock_name

            mock_executed_op = mock.MagicMock()
            mock_executed_op.job_name = 'myjob'
            mock_workspace = mock.MagicMock()
            mock_handle_storage_failure = mock.MagicMock()

            c._create_resource = mock_create_resource
            c._create_output_filename = mock_create_output_filename
            c._handle_storage_failure = mock_handle_storage_failure

            mock_retrieve_resource_class_standard_format.reset_mock()
            mock_initiate_resource_validation.reset_mock()

            # now call the converter
            mock_path = '/some/path'
            u = c.convert_output(mock_executed_op, mock_workspace, o, mock_path)
            self.assertIsNone(u)
            c._create_resource.assert_called_once_with(
                mock_executed_op,
                mock_workspace,
                mock_path,
                mock_name
            )
            mock_retrieve_resource_class_standard_format.assert_not_called()
            mock_initiate_resource_validation.assert_not_called()

    @mock.patch('api.converters.data_resource.ResourceMetadata')
    @mock.patch('api.converters.data_resource.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.data_resource.initiate_resource_validation')
    def test_single_local_output_converter_validation_failure(self, 
        mock_initiate_resource_validation,
        mock_retrieve_resource_class_standard_format,
        mock_resource_metadata_class):
        '''
        Tests the case where a validation failure occurs.
        '''

        mock_retrieve_resource_class_standard_format.return_value = 'TSV'

        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": False,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)

        c1 = LocalDockerSingleDataResourceConverter()
        for c in [c1,]:

            # mock out the actual creation of the Resource
            resource_pk = uuid.uuid4()
            mock_create_resource = mock.MagicMock()
            mock_resource = mock.MagicMock()
            mock_resource.pk = resource_pk
            mock_create_resource.return_value = mock_resource
            mock_create_output_filename = mock.MagicMock()
            mock_name = 'foo'
            mock_create_output_filename.return_value = mock_name
            mock_executed_op = mock.MagicMock()
            mock_executed_op.job_name = 'myjob'
            mock_workspace = mock.MagicMock()
            mock_handle_invalid_resource_type = mock.MagicMock()
            c._create_resource = mock_create_resource
            c._create_output_filename = mock_create_output_filename
            c._handle_invalid_resource_type = mock_handle_invalid_resource_type

            mock_retrieve_resource_class_standard_format.reset_mock()
            mock_initiate_resource_validation.reset_mock()

            mock_initiate_resource_validation.side_effect = Exception('!!!')

            # now call the converter
            mock_path = '/some/path'
            with self.assertRaisesRegex(OutputConversionException, 'Failed to validate'):
                u = c.convert_output(mock_executed_op, mock_workspace, o, mock_path)

            c._create_resource.assert_called_once_with(
                mock_executed_op,
                mock_workspace,
                mock_path,
                mock_name
            )
            mock_retrieve_resource_class_standard_format.assert_called_once_with('MTX')
            mock_initiate_resource_validation.assert_called_once_with(
                mock_resource,
                'MTX',
                'TSV'
            )

    def test_single_local_with_rt_input_input_converter(self):
        '''
        Tests that the converter can take a single Resource instance
        and return the local path AND resource type as a special delimited string
        '''
        # the validators will check the validity of the user inputs prior to 
        # calling the converter. Thus, we can use basically any Resource to test
        all_resources = Resource.objects.all()
        r = all_resources[0]
        rt = r.resource_type

        user_input = str(r.pk)
        mock_path = '/path/to/file.txt'
        mock_staging_dir = '/path/to/dir'
        mock_convert_resource_input = mock.MagicMock()
        mock_convert_resource_input.return_value = mock_path

        c = LocalDockerSingleDataResourceWithTypeConverter()
        c._convert_resource_input = mock_convert_resource_input

        x = c.convert_input( user_input, '', mock_staging_dir)
        delim = LocalDockerSingleDataResourceWithTypeConverter.DELIMITER
        expected_str = f'{mock_path}{delim}{rt}'
        self.assertEqual(x, expected_str)
        mock_convert_resource_input.assert_called_once_with(
            user_input, mock_staging_dir)

    def test_single_nextflow_input_converter(self):
        '''
        Tests that the converter can take a single Resource instance
        and return the path to a bucket-based file
        '''
        mock_input = str(uuid.uuid4())
        mock_path = '/path/to/file.txt'
        mock_staging_dir = '/some/staging_dir/'

        c = RemoteNextflowSingleDataResourceConverter()
        mock_convert_resource_input = mock.MagicMock()
        mock_convert_resource_input.return_value = mock_path
        c._convert_resource_input = mock_convert_resource_input

        x = c.convert_input( mock_input, '', mock_staging_dir)
        self.assertEqual(x,  mock_path)
        mock_convert_resource_input.assert_called_with(mock_input, mock_staging_dir)

    # TODO: re-enable once Nextflow runner is ready
    # def test_cromwell_converters_case1(self):
    #     '''
    #     Tests that the CromwellMultipleDataResourceConverter
    #     converter can take a list of Resource instances
    #     and return a properly formatted response. The response
    #     depends on the actual implementing class.

    #     Here we test with multiple input resource UUIDs
    #     '''
    #     uuid_list = [str(uuid.uuid4()) for i in range(3)]

    #     c = CromwellMultipleDataResourceConverter()
    #     mock_path_list = [
    #         '/path/to/a.txt',
    #         '/path/to/b.txt',
    #         '/path/to/c.txt',
    #     ]
    #     # patch a method on that class. This method is the
    #     # one which gets the Resource and gets the fully
    #     # resolved path in our storage system.
    #     c._convert_resource_input = mock.MagicMock()
    #     c._convert_resource_input.side_effect = mock_path_list
        
    #     x = c.convert_input( uuid_list, '', '/some/staging_dir/')
    #     self.assertEqual(x, mock_path_list)

    def test_multiple_resource_local_converter_case1(self):
        '''
        Tests that the converter can take a list of Resource instance UUIDs
        and return a properly formatted response.

        This covers the LocalDockerMultipleDataResourceConverter
        and derived classes
        '''
        mock_paths = ['/foo/bar1.txt', '/foo/bar2.txt', '/foo/bar3.txt']
        mock_staging_dir = '/some/staging_dir'
        mock_inputs = [str(uuid.uuid4()) for _ in range(len(mock_paths))]

        mock_convert_resource_input = mock.MagicMock()
        mock_convert_resource_input.side_effect = mock_paths

        c = LocalDockerMultipleDataResourceConverter()
        c._convert_resource_input = mock_convert_resource_input
        x = c.convert_input( mock_inputs, '', mock_staging_dir)
        self.assertEqual(x,  mock_paths)
        mock_convert_resource_input.assert_has_calls([
            mock.call(u, mock_staging_dir) for u in mock_inputs
        ])

        c = LocalDockerMultipleVariableDataResourceConverter()
        mock_convert_resource_input.reset_mock()
        mock_convert_resource_input.side_effect = mock_paths
        c._convert_resource_input = mock_convert_resource_input
        x = c.convert_input( mock_inputs, '', mock_staging_dir)
        self.assertEqual(x,  mock_paths)
        mock_convert_resource_input.assert_has_calls([
            mock.call(u, mock_staging_dir) for u in mock_inputs
        ])

        c = LocalDockerCsvResourceConverter()
        mock_convert_resource_input.reset_mock()
        mock_convert_resource_input.side_effect = mock_paths
        c._convert_resource_input = mock_convert_resource_input
        x = c.convert_input( mock_inputs, '', mock_staging_dir)
        expected = ','.join(mock_paths)
        self.assertEqual(x,  expected)
        mock_convert_resource_input.assert_has_calls([
            mock.call(u, mock_staging_dir) for u in mock_inputs
        ])

        c = LocalDockerSpaceDelimResourceConverter()
        mock_convert_resource_input.reset_mock()
        mock_convert_resource_input.side_effect = mock_paths
        c._convert_resource_input = mock_convert_resource_input
        x = c.convert_input( mock_inputs, '', mock_staging_dir)
        expected = ' '.join(mock_paths)
        self.assertEqual(x,  expected)
        mock_convert_resource_input.assert_has_calls([
            mock.call(u, mock_staging_dir) for u in mock_inputs
        ])

    def test_multiple_resource_local_converter_case2(self):
        '''
        Tests that the converter can take a "list" of a single 
        Resource instance UUID
        and return a properly formatted response.

        This covers the LocalDockerMultipleDataResourceConverter
        and derived classes
        '''
        mock_path = '/foo/bar1.txt'
        mock_staging_dir = '/some/staging_dir'
        mock_input = str(uuid.uuid4())

        mock_convert_resource_input = mock.MagicMock()
        mock_convert_resource_input.return_value = mock_path

        c = LocalDockerMultipleDataResourceConverter()
        c._convert_resource_input = mock_convert_resource_input
        x = c.convert_input( mock_input, '', mock_staging_dir)
        self.assertEqual(x,  [mock_path])
        mock_convert_resource_input.assert_called_once_with(
            mock_input, mock_staging_dir)

        c = LocalDockerCsvResourceConverter()
        mock_convert_resource_input.reset_mock()
        c._convert_resource_input = mock_convert_resource_input
        x = c.convert_input( mock_input, '', mock_staging_dir)
        self.assertEqual(x,  mock_path)
        mock_convert_resource_input.assert_called_once_with(
            mock_input, mock_staging_dir)

        c = LocalDockerSpaceDelimResourceConverter()
        mock_convert_resource_input.reset_mock()
        c._convert_resource_input = mock_convert_resource_input
        x = c.convert_input( mock_input, '', mock_staging_dir)
        self.assertEqual(x,  mock_path)
        mock_convert_resource_input.assert_called_once_with(
            mock_input, mock_staging_dir)

    @mock.patch('api.converters.data_resource.ResourceMetadata')
    @mock.patch('api.converters.data_resource.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.data_resource.initiate_resource_validation')
    def test_multiple_local_output_converter(self, 
        mock_initiate_resource_validation,
        mock_retrieve_resource_class_standard_format,
        mock_resource_metadata_class):
        '''
        Tests the proper steps are performed for converting 
        multiple local outputs.
        '''

        mock_retrieve_resource_class_standard_format.return_value = 'TSV'

        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": True,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)

        c1 = LocalDockerMultipleDataResourceConverter()
        # TODO: re-enable once Nextflow runner is ready
        # c2 = CromwellMultipleDataResourceConverter()
        # for c in [c1, c2]:
        for c in [c1,]:
            # mock there being two resources to create
            all_resource_pk = [
                uuid.uuid4(),
                uuid.uuid4()
            ]
            mock_create_resource = mock.MagicMock()
            mock_resource1 = mock.MagicMock()
            mock_resource1.pk = all_resource_pk[0]
            mock_resource2 = mock.MagicMock()
            mock_resource2.pk = all_resource_pk[1]
            mock_create_resource.side_effect = [mock_resource1, mock_resource2]
            mock_create_output_filename = mock.MagicMock()
            mock_name1 = 'foo'
            mock_name2 = 'bar'
            mock_create_output_filename.side_effect = [mock_name1, mock_name2]
            mock_executed_op = mock.MagicMock()
            mock_executed_op.job_name = 'myjob'
            mock_workspace = mock.MagicMock()
            c._create_resource = mock_create_resource
            c._create_output_filename = mock_create_output_filename

            # call reset on the mocked functions that are used 
            # on each loop:
            mock_retrieve_resource_class_standard_format.reset_mock()
            mock_initiate_resource_validation.reset_mock()

            # now call the converter
            mock_paths = ['/some/path1', 'some/path2']
            u = c.convert_output(mock_executed_op, mock_workspace, o, mock_paths)
            self.assertCountEqual(u, [str(x) for x in all_resource_pk])
            c._create_resource.assert_has_calls([
                mock.call(
                    mock_executed_op,
                    mock_workspace,
                    mock_paths[0],
                    mock_name1
                ),
                mock.call(
                    mock_executed_op,
                    mock_workspace,
                    mock_paths[1],
                    mock_name2
                ),
            ])

            mock_retrieve_resource_class_standard_format.assert_has_calls([
                mock.call('MTX'), mock.call('MTX')])

            mock_initiate_resource_validation.assert_has_calls([
                mock.call(
                    mock_resource1,
                    'MTX',
                    'TSV'),
                mock.call(
                    mock_resource2,
                    'MTX',
                    'TSV'),
            ])
            self.assertTrue(mock_resource1.is_active)
            mock_resource1.save.assert_called()
            self.assertTrue(mock_resource2.is_active)
            mock_resource2.save.assert_called()

    @mock.patch('api.converters.data_resource.ResourceMetadata')
    @mock.patch('api.converters.data_resource.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.data_resource.initiate_resource_validation')
    @mock.patch('api.converters.data_resource.delete_resource_by_pk')
    def test_multiple_local_output_converter_failure(self, 
        mock_delete_resource_by_pk,
        mock_initiate_resource_validation,
        mock_retrieve_resource_class_standard_format,
        mock_resource_metadata_class):
        '''
        Tests that we handle cleanup of other outputs if one of 
        them fails. Here, we mock there being a storage problem on
        one of multiple files corresponding to a single input.
        Since the output was marked as required, we check that
        an exception is raised and we delete the other resource
        so as not to create an incomplete output state.
        '''

        mock_retrieve_resource_class_standard_format.return_value = 'TSV'
        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": True,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)
        c1 = LocalDockerMultipleDataResourceConverter()
        # TODO: re-enable once Nextflow runner is ready
        # c2 = CromwellMultipleDataResourceConverter()
        # for c in [c1, c2]:
        for c in [c1,]:

            # mock there being two resources to create
            all_resource_pk = [
                uuid.uuid4(),
                uuid.uuid4()
            ]
            mock_create_resource = mock.MagicMock()
            mock_resource1 = mock.MagicMock()
            mock_resource1.pk = all_resource_pk[0]
            mock_create_resource.side_effect = [
                mock_resource1, StorageException('!!')]
            mock_create_output_filename = mock.MagicMock()
            mock_name1 = 'foo'
            mock_name2 = 'bar'
            mock_create_output_filename.side_effect = [mock_name1, mock_name2]
            mock_executed_op = mock.MagicMock()
            mock_executed_op.job_name = 'myjob'
            mock_workspace = mock.MagicMock()
            c._create_resource = mock_create_resource
            c._create_output_filename = mock_create_output_filename

            # call reset on the mocked functions that are used 
            # on each loop:
            mock_retrieve_resource_class_standard_format.reset_mock()
            mock_initiate_resource_validation.reset_mock()

            # now call the converter
            # even though this test is checking for proper behavior with
            # both local and Cromwell converters, these paths don't 
            # actually matter in their content:
            mock_paths = ['/some/path1', 'some/path2']
            with self.assertRaisesRegex(OutputConversionException, 'Failed to convert'):
                c.convert_output(mock_executed_op, mock_workspace, o, mock_paths)
            c._create_resource.assert_has_calls([
                mock.call(
                    mock_executed_op,
                    mock_workspace,
                    mock_paths[0],
                    mock_name1
                ),
                mock.call(
                    mock_executed_op,
                    mock_workspace,
                    mock_paths[1],
                    mock_name2
                ),
            ])
            mock_retrieve_resource_class_standard_format.assert_has_calls([
                mock.call('MTX')])
            mock_initiate_resource_validation.assert_has_calls([
                mock.call(
                    mock_resource1,
                    'MTX',
                    'TSV'),
            ])
            self.assertTrue(mock_resource1.is_active)
            mock_resource1.save.assert_called()
            mock_delete_resource_by_pk.assert_has_calls([
                mock.call(str(all_resource_pk[0]))
            ])

    @mock.patch('api.converters.data_resource.ResourceMetadata')
    @mock.patch('api.converters.data_resource.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.data_resource.initiate_resource_validation')
    @mock.patch('api.converters.data_resource.delete_resource_by_pk')
    def test_multiple_output_converter_failure_for_optional(self, 
        mock_delete_resource_by_pk,
        mock_initiate_resource_validation,
        mock_retrieve_resource_class_standard_format,
        mock_resource_metadata_class):
        '''
        Here, we mock there being a storage problem on
        one of multiple files corresponding to a single input.
        Since the output was NOT marked as required, we don't
        raise any exceptions
        '''

        mock_retrieve_resource_class_standard_format.return_value = 'TSV'

        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': False, #<--- important!
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": True,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)
        c1 = LocalDockerMultipleDataResourceConverter()
        # c2 = CromwellMultipleDataResourceConverter()
        # for c in [c1, c2]:
        for c in [c1,]:

            # mock there being two resources to create
            all_resource_pk = [
                uuid.uuid4(),
                uuid.uuid4()
            ]
            mock_create_resource = mock.MagicMock()
            mock_resource1 = mock.MagicMock()
            mock_resource1.pk = all_resource_pk[0]
            mock_create_resource.side_effect = [
                mock_resource1, StorageException('!!')]
            mock_create_output_filename = mock.MagicMock()
            mock_name1 = 'foo'
            mock_name2 = 'bar'
            mock_create_output_filename.side_effect = [mock_name1, mock_name2]
            mock_executed_op = mock.MagicMock()
            mock_executed_op.job_name = 'myjob'
            mock_workspace = mock.MagicMock()
            mock_clean = mock.MagicMock()
            c._create_resource = mock_create_resource
            c._create_output_filename = mock_create_output_filename
            c._cleanup_other_outputs = mock_clean

            # call reset on the mocked functions that are used 
            # on each loop:
            mock_retrieve_resource_class_standard_format.reset_mock()
            mock_initiate_resource_validation.reset_mock()

            # now call the converter
            # even though this test is checking for proper behavior with
            # both local and Cromwell converters, these paths don't 
            # actually matter in their content:
            mock_paths = ['/some/path1', 'some/path2']
            u = c.convert_output(mock_executed_op, mock_workspace, o, mock_paths)
            self.assertCountEqual(u, [str(all_resource_pk[0]), None])
            c._create_resource.assert_has_calls([
                mock.call(
                    mock_executed_op,
                    mock_workspace,
                    mock_paths[0],
                    mock_name1
                ),
                mock.call(
                    mock_executed_op,
                    mock_workspace,
                    mock_paths[1],
                    mock_name2
                ),
            ])
            mock_retrieve_resource_class_standard_format.assert_has_calls([
                mock.call('MTX')])
            mock_initiate_resource_validation.assert_has_calls([
                mock.call(
                    mock_resource1,
                    'MTX',
                    'TSV'),
            ])
            self.assertTrue(mock_resource1.is_active)
            mock_resource1.save.assert_called()
            mock_clean.assert_not_called()

    # TODO: re-enable once Nextflow runner is ready
    # def test_cromwell_converters_case2(self):
    #     '''
    #     Tests that the multiple DataResource converters 
    #     can take a list of Resource instances
    #     and return a properly formatted response (which depends
    #     on the converter class)

    #     Here, we pass only a single resource UUID. 
    #     For example, we may have a WDL workflow which can accept >=1 
    #     inputs as an array. Here we test that passing a single input
    #     results in a list of paths of length 1.
    #     '''

    #     all_resources = Resource.objects.all()
    #     r = all_resources[0]

    #     mock_path = '/path/to/a.txt'
    #     # instantiate and patch a method on that class:
    #     c = CromwellMultipleDataResourceConverter()
    #     mock_convert_resource_input = mock.MagicMock()
    #     mock_convert_resource_input.return_value = mock_path
    #     c._convert_resource_input = mock_convert_resource_input
    #     mock_staging_dir = '/some/staging_dir/'
    #     x = c.convert_input(str(r.pk), '', mock_staging_dir)
    #     # response should be a LIST of paths.
    #     self.assertEqual(x, [mock_path])
    #     mock_convert_resource_input.assert_called_once_with(
    #         str(r.pk),
    #         mock_staging_dir
    #     )

class TestJsonConverter(BaseAPITestCase):

    def test_json_output(self):
        '''
        Tools can output a nested json data structure. That is,
        for local outputs, we have `outputs.json` which contains
        information like paths to output files, etc.

        Inside that, a key can address a json structure.

        Note that failures to properly nest the JSON will raise 
        exceptions when the file is read by the built-in json lib.

        This catches the situation where the addressed item is not,
        in fact, valid json
        '''
        
        c = JsonConverter()
        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        o = None
        j = 'abc'
        with self.assertRaises(json.decoder.JSONDecodeError):
            c.convert_output(mock_executed_op, mock_workspace, o, j)


class TestRemoteNextflowSingleVariableDataResourceConverter(BaseAPITestCase):

    def test_single_output_converter(self):
        '''
        Tests the case where we convert a single output file generated
        by a process using the nextflow runner
        '''
        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "VariableDataResource",
                "many": False,
                "resource_types": ["MTX", "I_MTX"]
            }
        }
        o = OperationOutput(d)

        c = RemoteNextflowSingleVariableDataResourceConverter()

        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        mock_attempt_resource_addition = mock.MagicMock()
        mock_resource = mock.MagicMock()
        mock_attempt_resource_addition.return_value = mock_resource
        c._attempt_resource_addition = mock_attempt_resource_addition
        # now call the converter. Note that the way Nextflow works is that
        # we place output files in a directory named after the output key.
        # To accommodate both single and multiple-resource outputs, the 
        # Nextflow runners return a list of paths (even if there is only one file)
        p = '/some/path'
        mock_paths = [{'path':p, 'resource_type': 'MTX'}]
        u = c.convert_output(mock_executed_op, mock_workspace, o, mock_paths)

        mock_attempt_resource_addition.assert_called_once_with(
            mock_executed_op, mock_workspace, p, 'MTX', True
        )

    def test_bad_call_to_single_output_converter(self):
        '''
        Tests the case where we convert a single output file generated
        by a process using the nextflow runner
        '''
        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "VariableDataResource",
                "many": False,
                "resource_types": ["MTX", "I_MTX"]
            }
        }
        o = OperationOutput(d)

        c = RemoteNextflowSingleVariableDataResourceConverter()

        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        mock_attempt_resource_addition = mock.MagicMock()
        mock_resource = mock.MagicMock()
        mock_attempt_resource_addition.return_value = mock_resource
        c._attempt_resource_addition = mock_attempt_resource_addition
        # now call the converter. Note that the way Nextflow works is that
        # we place output files in a directory named after the output key.
        # To accommodate both single and multiple-resource outputs, the 
        # Nextflow runners return a list of paths (even if there is only one file)
        # Here we test that it fails if we pass a single item
        mock_paths = {'path':'abc', 'resource_type': 'MTX'}
        with self.assertRaisesRegex(OutputConversionException, 'expects a list'):
            c.convert_output(mock_executed_op, mock_workspace, o, mock_paths) 

    @mock.patch('api.converters.data_resource.ResourceMetadata')
    @mock.patch('api.converters.data_resource.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.data_resource.initiate_resource_validation')
    def test_single_local_output_converter_validation_failure(self, 
        mock_initiate_resource_validation,
        mock_retrieve_resource_class_standard_format,
        mock_resource_metadata_class):
        '''
        Tests the case where a validation failure occurs.
        '''

        mock_retrieve_resource_class_standard_format.return_value = 'TSV'

        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "VariableDataResource",
                "many": False,
                "resource_types": ["MTX", "I_MTX"]
            }
        }
        o = OperationOutput(d)

        c = RemoteNextflowSingleVariableDataResourceConverter()

        # mock out the actual creation of the Resource
        resource_pk = uuid.uuid4()
        mock_create_resource = mock.MagicMock()
        mock_resource = mock.MagicMock()
        mock_resource.pk = resource_pk
        mock_create_resource.return_value = mock_resource
        mock_create_output_filename = mock.MagicMock()
        mock_name = 'foo'
        mock_create_output_filename.return_value = mock_name
        mock_executed_op = mock.MagicMock()
        mock_executed_op.job_name = 'myjob'
        mock_workspace = mock.MagicMock()
        mock_handle_invalid_resource_type = mock.MagicMock()
        c._create_resource = mock_create_resource
        c._create_output_filename = mock_create_output_filename
        c._handle_invalid_resource_type = mock_handle_invalid_resource_type

        mock_retrieve_resource_class_standard_format.reset_mock()
        mock_initiate_resource_validation.reset_mock()

        mock_initiate_resource_validation.side_effect = Exception('!!!')

        # now call the converter. Note that the way Nextflow works is that
        # we place output files in a directory named after the output key.
        # To accommodate both single and multiple-resource outputs, the 
        # Nextflow runners return a list of paths (even if there is only one file)
        p = '/some/path'
        mock_paths = [{'path':p, 'resource_type': 'MTX'}]
        with self.assertRaisesRegex(OutputConversionException, 'Failed to validate'):
            u = c.convert_output(mock_executed_op, mock_workspace, o, mock_paths)

        c._create_resource.assert_called_once_with(
            mock_executed_op,
            mock_workspace,
            p,
            mock_name
        )
        mock_retrieve_resource_class_standard_format.assert_called_once_with('MTX')
        mock_initiate_resource_validation.assert_called_once_with(
            mock_resource,
            'MTX',
            'TSV'
        )

class TestNextflowSingleResourceConverter(BaseAPITestCase):

    def test_single_output_converter(self):
        '''
        Tests the case where we convert a single output file generated
        by a process using the nextflow runner
        '''
        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": False,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)

        c = RemoteNextflowSingleDataResourceConverter()

        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        mock_attempt_resource_addition = mock.MagicMock()
        mock_resource = mock.MagicMock()
        mock_attempt_resource_addition.return_value = mock_resource
        c._attempt_resource_addition = mock_attempt_resource_addition
        # now call the converter. Note that the way Nextflow works is that
        # we place output files in a directory named after the output key.
        # To accommodate both single and multiple-resource outputs, the 
        # Nextflow runners return a list of paths (even if there is only one file)
        p = '/some/path'
        mock_paths = [p,]
        u = c.convert_output(mock_executed_op, mock_workspace, o, mock_paths)

        mock_attempt_resource_addition.assert_called_once_with(
            mock_executed_op, mock_workspace, p, 'MTX', True
        )

    def test_single_output_converter_fails_with_multiple(self):
        '''
        Tests the case where we somehow have a list of >1 items that
        is passed to a RemoteNextflowSingleDataResourceConverter.
        '''
        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": False,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)

        c = RemoteNextflowSingleDataResourceConverter()

        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        mock_attempt_resource_addition = mock.MagicMock()
        mock_resource = mock.MagicMock()
        mock_attempt_resource_addition.return_value = mock_resource
        c._attempt_resource_addition = mock_attempt_resource_addition
        # now call the converter. Note that the way Nextflow works is that
        # we place output files in a directory named after the output key.
        # To accommodate both single and multiple-resource outputs, the 
        # Nextflow runners return a list of paths (even if there is only one file)
        p = '/some/path'
        mock_paths = [p, 'other_path']
        with self.assertRaises(OutputConversionException):
            c.convert_output(mock_executed_op, mock_workspace, o, mock_paths)

        mock_attempt_resource_addition.assert_not_called()

    def test_bad_call_to_single_local_output_converter(self):
        '''
        Tests the case where we convert a single output file generated
        by a process using the nextflow runner
        '''
        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": False,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)

        c = RemoteNextflowSingleDataResourceConverter()

        mock_executed_op = mock.MagicMock()
        mock_workspace = mock.MagicMock()
        mock_attempt_resource_addition = mock.MagicMock()
        mock_resource = mock.MagicMock()
        mock_attempt_resource_addition.return_value = mock_resource
        c._attempt_resource_addition = mock_attempt_resource_addition
        # now call the converter. Note that the way Nextflow works is that
        # we place output files in a directory named after the output key.
        # To accommodate both single and multiple-resource outputs, the 
        # Nextflow runners return a list of paths (even if there is only one file)
        # Here we test that it fails if we pass a single item
        p = '/some/path'
        with self.assertRaisesRegex(OutputConversionException, 'expects a list'):
            c.convert_output(mock_executed_op, mock_workspace, o, p) 

    @mock.patch('api.converters.data_resource.ResourceMetadata')
    @mock.patch('api.converters.data_resource.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.data_resource.initiate_resource_validation')
    def test_single_local_output_converter_validation_failure(self, 
        mock_initiate_resource_validation,
        mock_retrieve_resource_class_standard_format,
        mock_resource_metadata_class):
        '''
        Tests the case where a validation failure occurs.
        '''

        mock_retrieve_resource_class_standard_format.return_value = 'TSV'

        # need a valid data_resources.operation_output.OperationOutput
        # instance:
        d = {
            'required': True,
            'converter': '',
            'spec': {
                "attribute_type": "DataResource",
                "many": False,
                "resource_type": "MTX"
            }
        }
        o = OperationOutput(d)

        c = RemoteNextflowSingleDataResourceConverter()

        # mock out the actual creation of the Resource
        resource_pk = uuid.uuid4()
        mock_create_resource = mock.MagicMock()
        mock_resource = mock.MagicMock()
        mock_resource.pk = resource_pk
        mock_create_resource.return_value = mock_resource
        mock_create_output_filename = mock.MagicMock()
        mock_name = 'foo'
        mock_create_output_filename.return_value = mock_name
        mock_executed_op = mock.MagicMock()
        mock_executed_op.job_name = 'myjob'
        mock_workspace = mock.MagicMock()
        mock_handle_invalid_resource_type = mock.MagicMock()
        c._create_resource = mock_create_resource
        c._create_output_filename = mock_create_output_filename
        c._handle_invalid_resource_type = mock_handle_invalid_resource_type

        mock_retrieve_resource_class_standard_format.reset_mock()
        mock_initiate_resource_validation.reset_mock()

        mock_initiate_resource_validation.side_effect = Exception('!!!')

        # now call the converter. Note that the way Nextflow works is that
        # we place output files in a directory named after the output key.
        # To accommodate both single and multiple-resource outputs, the 
        # Nextflow runners return a list of paths (even if there is only one file)
        p = '/some/path'
        mock_paths = [p,]
        with self.assertRaisesRegex(OutputConversionException, 'Failed to validate'):
            u = c.convert_output(mock_executed_op, mock_workspace, o, mock_paths)

        c._create_resource.assert_called_once_with(
            mock_executed_op,
            mock_workspace,
            p,
            mock_name
        )
        mock_retrieve_resource_class_standard_format.assert_called_once_with('MTX')
        mock_initiate_resource_validation.assert_called_once_with(
            mock_resource,
            'MTX',
            'TSV'
        )


class TestECSSingleVariableDataResourceConverter(BaseAPITestCase):
    @mock.patch('api.converters.data_resource.get_resource_by_pk')
    @mock.patch('api.converters.data_resource.default_storage')
    def test_input_conversion(self, mock_storage, mock_get_resource_by_pk):
        mock_resource = mock.MagicMock()
        mock_resource.datafile.name = 'some-name'
        mock_get_resource_by_pk.return_value =  mock_resource
        mock_storage.get_absolute_path.return_value = 's3://some-bucket/some-obj'
        c = ECSSingleDataResourceConverter()
        c.convert_input('some-pk', '', '')
        mock_storage.get_absolute_path.assert_called_with('some-name')
        mock_get_resource_by_pk.assert_called_with('some-pk')   

class TestNextflowSingleResourceConverter(BaseAPITestCase):

    @mock.patch('api.converters.data_resource.get_resource_by_pk')
    @mock.patch('api.converters.data_resource.default_storage')
    def test_input_conversion(self, mock_storage, mock_get_resource_by_pk):
        mock_resource = mock.MagicMock()
        mock_resource.datafile.name = 'some-name'
        mock_get_resource_by_pk.return_value =  mock_resource
        mock_storage.get_absolute_path.return_value = 's3://some-bucket/some-obj'
        c = ECSSingleDataResourceConverter()
        c.convert_input('some-pk', '', '')
        mock_storage.get_absolute_path.assert_called_with('some-name')
        mock_get_resource_by_pk.assert_called_with('some-pk')
    