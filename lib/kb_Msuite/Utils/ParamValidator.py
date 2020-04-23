# -*- coding: utf-8 -*-
import sys
import time

def log(message, prefix_newline=False):
    """Logging function, provides a hook to suppress or redirect log messages."""
    print(('\n' if prefix_newline else '') + '{0:.2f}'.format(time.time()) + ': ' + str(message))
    sys.stdout.flush()


def validate_params(params):
    """
    validate and normalize the checkM input params

    """

    # 0) validate basic parameters
    if 'input_ref' not in params:
        raise ValueError('input_ref field was not set in params for run_checkM_lineage_wf')
    if 'workspace_name' not in params:
        raise ValueError('workspace_name field was not set in params for run_checkM_lineage_wf')

    if 'reduced_tree' in params:
        if params['reduced_tree'] is None or int(params['reduced_tree']) != 1:
            del params['reduced_tree']

    validator = {
        'input_ref': {
            'required': True,
            'type': 'string',
        },
        'workspace_name': {
            'required': True,
            'type': 'string',
        },
        'reduced_tree': {
            'type': 'boolean',
        },
        'save_output_dir': {
            'type': 'boolean',
        },
        'save_plots_dir': {
            'type': 'boolean',
        },
        'threads': {
            'type': 'int',
        },

#         float completeness_perc;   /* 0-100, default 95% */
#         float contamination_perc;  /* 0-100, default: 2% */
#         string output_filtered_binnedcontigs_obj_name;

        # with filters:
        'completeness_perc': {
            'type': 'float',
        },
        'contamination_perc': {
            'type': 'float',
        },
        'output_filtered_binnedcontigs_obj_name': {
            'type': 'string',
        },
    }

    '''
    /*
        input_ref - reference to the input Assembly, AssemblySet, Genome, GenomeSet, or BinnedContigs data
    */
    typedef structure {
        string input_ref;
        string workspace_name;

        boolean reduced_tree;
        boolean save_output_dir;
        boolean save_plots_dir;

        int threads;

    } CheckMLineageWfParams;

    /*
        input_ref - reference to the input BinnedContigs data
    */
    typedef structure {
        string input_ref;
        string workspace_name;

        boolean reduced_tree;
        boolean save_output_dir;
        boolean save_plots_dir;

        int threads;

        float completeness_perc;   /* 0-100, default 95% */
        float contamination_perc;  /* 0-100, default: 2% */
        string output_filtered_binnedcontigs_obj_name;
    } CheckMLineageWf_withFilter_Params;

    '''
