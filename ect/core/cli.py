"""
Module Description
==================

This module provides ECT's command-line interface (CLI).

Module Reference
================
"""

import argparse
import sys

import ect.core.op
import ect.core.plugin


def main(args=None):
    """
    The CLI's entry point function.

    :param args: list of command-line arguments of type ``str``.
    :return: exit code
    """

    if not args:
        args = sys.argv[1:]

    print('ESA CCI Toolbox (ECT) command-line interface, version %s' % ect.__version__)

    #
    # Configure and run argument parser
    #
    parser = argparse.ArgumentParser(description='Generates a new CAB-LAB data cube or updates an existing one.')
    parser.add_argument('-l', '--list', action='store_true',
                        help="list all available readers, writers, processors")
    parser.add_argument('-c', '--config-file', metavar='CONFIG',
                        help="ECT configuration file")
    parser.add_argument('target_dir', metavar='TARGET', nargs='?',
                        help="default target directory")
    parser.add_argument('source_paths', metavar='SOURCE', nargs='*',
                        help='sources')
    args_obj = parser.parse_args(args)

    target_dir = args_obj.target_dir
    source_paths = args_obj.source_paths
    config_file = args_obj.config_file
    list_plugins = args_obj.list

    print('config_file:', config_file)
    print('target_dir:', target_dir)
    print('source_paths:', source_paths)
    print('list_plugins:', list_plugins)

    if list_plugins:
        plugin_registrations = ect.core.plugin.REGISTRY
        print('-' * 80)
        print('Registered ECT plugins (%s):' % len(plugin_registrations))
        print('-' * 80)
        for entry_point_name in plugin_registrations.keys():
            print(entry_point_name)

        op_registrations = ect.core.op.REGISTRY.op_registrations
        print('-' * 80)
        print('Registered ECT operations (%d):' % len(op_registrations))
        print('-' * 80)
        for qualified_name in op_registrations.keys():
            print(qualified_name)

        print('-' * 80)

    return 0


if __name__ == '__main__':
    main()
