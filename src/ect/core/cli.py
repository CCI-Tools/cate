"""
This module provides ECT's command-line interface.
"""

import argparse
import sys

import ect.core
import ect.core.plugin


def _print_dict(n, d):
    print('ECT %s (%d):' % (n, len(d)))
    for k, v in d.items():
        print('  %s: %s' % (k, v))


def main(args=None):
    """
    ECT's entry point of the command-line interface.
    :param args: command-line arguments
    :return: exit code
    """

    if not args:
        args = sys.argv[1:]

    print('ESA CCI Toolbox (ECT) command-line interface, version %s' % ect.__VERSION__)

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
        _print_dict('readers', ect.core.plugin.CONTEXT.readers)
        _print_dict('writers', ect.core.plugin.CONTEXT.writers)
        _print_dict('processors', ect.core.plugin.CONTEXT.processors)

    return 0


if __name__ == '__main__':
    main()
