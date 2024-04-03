import logging

import numpy as np
import pandas as pd

from constants import FEATURE_TABLE_KEY, \
    POSITIVE_INF_MARKER, \
    NEGATIVE_INF_MARKER
from data_structures.attribute_types import PositiveFloatAttribute, \
    BoundedFloatAttribute

from resource_types import get_resource_type_instance

logger = logging.getLogger(__name__)


def volcano_subset(resource, query_params):
    '''
    When the frontend wants to create a volcano plot, it does not
    need ALL the data, most of which is concentrated around the origin
    (where log-pval ~= 0 and lfc ~= 0).

    Given the p-value and lfc values, this draws a random subset
    of the "uninteresting" data
    '''

    try:
        lfc = PositiveFloatAttribute(float(query_params['lfc']))
        lfc = lfc.value
    except KeyError:
        raise Exception('You must supply a "lfc" parameter')
    except Exception:
        raise Exception('The parameter "lfc" could not be'
                        ' parsed as a positive float.')

    try:
        p = BoundedFloatAttribute(float(query_params['pval']), min=0.0, max=1.0)
        p = p.value
    except KeyError:
        raise Exception('You must supply a "pval" parameter')
    except Exception:
        raise Exception('The parameter "pval" could not be'
                        ' parsed as a positive float between zero and 1.')

    try:
        c = BoundedFloatAttribute(float(query_params['fraction']), min=0.0, max=1.0)
        c = c.value
    except KeyError:
        c = 0.01
    except Exception:
        raise Exception('The parameter "fraction" could not be'
                        ' parsed as a positive float between zero and 1.')

    acceptable_resource_types = [
        FEATURE_TABLE_KEY
    ]
    if not resource.resource_type in acceptable_resource_types:
        raise Exception('Not an acceptable resource type for this function.')

    resource_type_instance = get_resource_type_instance(resource.resource_type)
    df = resource_type_instance.get_contents(resource)

    # there MUST be padj and log2FoldChange columns to filter on
    required_cols = ['padj','log2FoldChange']
    if not all([x in df.columns for x in required_cols]):
        raise Exception('The table you are filtering must have both'
                        ' a "padj" and "log2FoldChange" column.')

    pval_pass = df['padj'] <= p
    lfc_pass = df['log2FoldChange'].apply(lambda x: np.abs(x) >= lfc)
    interesting = pval_pass & lfc_pass
    interesting_subset = df.loc[interesting]
    unintersting_subset = df.loc[~interesting].sample(frac=c)
    final_df = pd.concat([interesting_subset, unintersting_subset], axis=0)
    return resource_type_instance.to_json(final_df)