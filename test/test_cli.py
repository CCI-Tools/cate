from unittest import TestCase

from ect.core import cli


class CliTest(TestCase):
    def test_main(self):
        cli.main(args=['--list'])
