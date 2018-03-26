# -*- coding: utf-8 -*-

import io
import json
import os
import subprocess
import tempfile

from argparse import RawDescriptionHelpFormatter

from cookiecutter.generate import generate_context, generate_files
from cookiecutter.prompt import prompt_for_config
from pathlib2 import Path

from ..exposures.manager import OasisExposuresManager
from ..model_execution.bash import genbash
from ..model_execution.runner import run
from ..model_execution.bin import create_binary_files, prepare_model_run_directory, prepare_model_run_inputs
from ..utils.exceptions import OasisException
from ..utils.values import get_utctimestamp
from ..keys.lookup import OasisKeysLookupFactory
from .cleaners import PathCleaner, as_path, slug
from .base import OasisBaseCommand, InputValues


class GenerateKeysCmd(OasisBaseCommand):
    """
    Generate and write Oasis keys (area peril ID, vulnerability ID) for a model.

    The command line arguments can be supplied in the configuration file
    (``oasislmf.json`` by default or specified with the ``--config`` flag).
    Run ``oasislmf config --help`` for more information.

    Keys records returned by an Oasis keys lookup service (see the PiWind
    lookup service for reference) will be Python dicts with the following
    structure
    ::

        {
            "id": <loc. ID>,
            "peril_id": <Oasis peril type ID - oasis_utils/oasis_utils.py>,
            "coverage": <Oasis coverage type ID - see oasis_utils/oasis_utils.py>,
            "area_peril_id": <area peril ID>,
            "vulnerability_id": <vulnerability ID>,
            "message": <lookup status message>,
            "status": <lookup status code - see oasis_utils/oasis_utils.py>
        }

    The script can generate keys records in this format, and write them to file.

    For model loss calculations however ``ktools`` requires a keys CSV file with
    the following format
    ::

        LocID,PerilID,CoverageID,AreaPerilID,VulnerabilityID
        ..
        ..

    where the headers correspond to the relevant Oasis keys record fields.
    The script can also generate and write Oasis keys files.
    """
    formatter_class = RawDescriptionHelpFormatter

    def add_args(self, parser):
        """
        Adds arguments to the argument parser.

        :param parser: The argument parser object
        :type parser: ArgumentParser
        """
        super(GenerateKeysCmd, self).add_args(parser)

        parser.add_argument(
            '-o', '--output-file-path', default=None,
            help='Keys records output file path',
        )
        parser.add_argument(
            '-k', '--keys-data-path', default=None,
            help='Keys data directory path',
        )
        parser.add_argument(
            '-v', '--model-version-file-path', default=None,
            help='Model version file path',
        )
        parser.add_argument(
            '-l', '--lookup-package-path', default=None,
            help='Keys data directory path',
        )
        parser.add_argument(
            '-t', '--output-format', choices=['oasis_keys', 'json_keys'],
            help='Keys records file output format',
        )
        parser.add_argument(
            '-e', '--model-exposures-file-path', default=None, help='Keys records file output format',
        )
        parser.add_argument(
            '-s', '--successes-only', action='store_true', help='Generate lookup records only for successful exposure lookups',
        )
        parser.set_defaults(successes_only=False)

    def action(self, args):
        """
        Generate and write Oasis keys (area peril ID, vulnerability ID) for a model.

        :param args: The arguments from the command line
        :type args: Namespace
        """
        inputs = InputValues(args)
        model_exposures_file_path = as_path(inputs.get('model_exposures_file_path', required=True, is_path=True), 'Model exposures')
        keys_data_path = as_path(inputs.get('keys_data_path', required=True, is_path=True), 'Keys data')
        version_file_path = as_path(inputs.get('model_version_file_path', required=True, is_path=True), 'Version file')
        lookup_package_path = as_path(inputs.get('lookup_package_path', required=True, is_path=True), 'Lookup package')

        output_format = inputs.get('output_format', default='oasis_keys')
        successes_only = inputs.get('successes_only', default=True)

        self.logger.info('Getting model info and creating lookup service instance')
        model_info, model_klc = OasisKeysLookupFactory.create(
            model_keys_data_path=keys_data_path,
            model_version_file_path=version_file_path,
            lookup_package_path=lookup_package_path,
        )
        self.logger.info('\t{}, {}'.format(model_info, model_klc))

        utcnow = get_utctimestamp(fmt='%Y%m%d%H%M%S')
        default_output_file_name = '{}-{}-{}-keys-{}.{}'.format(model_info['supplier_id'].lower(), model_info['model_id'].lower(), model_info['model_version_id'], utcnow, 'csv' if output_format == 'oasis_keys' else 'json')
           
        output_file_path = as_path(inputs.get('output_file_path', default=default_output_file_name.format(utcnow), required=False, is_path=True), 'Output file path', preexists=False)

        self.logger.info('Saving keys records to file')
        f, n = OasisKeysLookupFactory.save_keys(
            lookup=model_klc,
            model_exposures_file_path=model_exposures_file_path,
            output_file_path=output_file_path,
            output_format=output_format,
            success_only=successes_only
        )
        self.logger.info('{} keys records saved to file {}'.format(n, f))


class GenerateOasisFilesCmd(OasisBaseCommand):
    """
    Generate Oasis files (items, coverages, GUL summary) for a model

    The command line arguments can be supplied in the configuration file
    (``oasislmf.json`` by default or specified with the ``--config`` flag).
    """
    def add_args(self, parser):
        """
        Adds arguments to the argument parser.

        :param parser: The argument parser object
        :type parser: ArgumentParser
        """
        super(GenerateOasisFilesCmd, self).add_args(parser)

        parser.add_argument('-o', '--oasis-files-path', default=None, help='Path to Oasis files')
        parser.add_argument('-k', '--keys-data-path', default=None, help='Path to Oasis files')
        parser.add_argument('-v', '--model-version-file-path', default=None, help='Model version file path')
        parser.add_argument('-l', '--lookup-package-path', default=None, help='Keys data directory path')
        parser.add_argument(
            '-p', '--canonical-exposures-profile-json-path', default=None,
            help='Path of the supplier canonical exposures profile JSON file'
        )
        parser.add_argument('-e', '--source-exposures-file-path', default=None, help='Source exposures file path')
        parser.add_argument(
            '-a', '--source-exposures-validation-file-path', default=None,
            help='Source exposures validation file (XSD) path'
        )
        parser.add_argument(
            '-b', '--source-to-canonical-exposures-transformation-file-path', default=None,
            help='Source -> canonical exposures transformation file (XSLT) path'
        )
        parser.add_argument(
            '-c', '--canonical-exposures-validation-file-path', default=None,
            help='Canonical exposures validation file (XSD) path'
        )
        parser.add_argument(
            '-d', '--canonical-to-model-exposures-transformation-file-path', default=None,
            help='Canonical exposures validation file (XSD) path'
        )

    def action(self, args):
        """
        Generate Oasis files (items, coverages, GUL summary) for a model

        :param args: The arguments from the command line
        :type args: Namespace
        """
        inputs = InputValues(args)

        utcnow = get_utctimestamp(fmt='%Y%m%d%H%M%S')
        default_oasis_files_path = os.path.join(os.getcwd(), 'runs', 'OasisFiles-{}'.format(utcnow))
        oasis_files_path = as_path(inputs.get('oasis_files_path', is_path=True, default=default_oasis_files_path), 'Oasis file', preexists=False)
        keys_data_path = as_path(inputs.get('keys_data_path', required=True, is_path=True), 'Keys data')
        model_version_file_path = as_path(inputs.get('model_version_file_path', required=True, is_path=True), 'Model version file')
        lookup_package_file_path = as_path(inputs.get('lookup_package_path', required=True, is_path=True), 'Lookup package file')
        canonical_exposures_profile_json_path = as_path(
            inputs.get('canonical_exposures_profile_json_path', required=True, is_path=True),
            'Canonical exposures profile json'
        )
        source_exposures_file_path = as_path(inputs.get('source_exposures_file_path', required=True, is_path=True), 'Source exposures')
        source_exposures_validation_file_path = as_path(
            inputs.get('source_exposures_validation_file_path', required=True, is_path=True),
            'Source exposures validation file'
        )
        source_to_canonical_exposures_transformation_file_path = as_path(
            inputs.get('source_to_canonical_exposures_transformation_file_path', required=True, is_path=True),
            'Source to canonical exposures transformation'
        )
        canonical_exposures_validation_file_path = as_path(
            inputs.get('canonical_exposures_validation_file_path', required=True, is_path=True),
            'Canonical exposures validation file'
        )
        canonical_to_model_exposures_transformation_file_path = as_path(
            inputs.get('canonical_to_model_exposures_transformation_file_path', required=True, is_path=True),
            'Canonical to model exposures transformation file'
        )

        self.logger.info('Getting model info and creating lookup service instance')
        model_info, model_klc = OasisKeysLookupFactory.create(
            model_keys_data_path=keys_data_path,
            model_version_file_path=model_version_file_path,
            lookup_package_path=lookup_package_file_path,
        )
        self.logger.info('\t{}, {}'.format(model_info, model_klc))

        self.logger.info('Creating Oasis model object')
        model = OasisExposuresManager().create(
            model_supplier_id=model_info['supplier_id'],
            model_id=model_info['model_id'],
            model_version_id=model_info['model_version_id'],
            resources={
                'lookup': model_klc,
                'oasis_files_path': oasis_files_path,
                'canonical_exposures_profile_json_path': canonical_exposures_profile_json_path,
                'source_exposures_validation_file_path': source_exposures_validation_file_path,
                'source_to_canonical_exposures_transformation_file_path': source_to_canonical_exposures_transformation_file_path,
                'canonical_exposures_validation_file_path': canonical_exposures_validation_file_path,
                'canonical_to_model_exposures_transformation_file_path': canonical_to_model_exposures_transformation_file_path,
            }
        )
        self.logger.info('\t{}'.format(model))

        self.logger.info('Setting up Oasis files directory for model {}'.format(model.key))
        Path(oasis_files_path).mkdir(parents=True, exist_ok=True)

        self.logger.info('Generating Oasis files for model')
        oasis_files = OasisExposuresManager().start_files_pipeline(
            oasis_model=model,
            oasis_files_path=oasis_files_path,
            source_exposures_path=source_exposures_file_path,
            logger=self.logger,
        )

        self.logger.info('Generated Oasis files for model: {}'.format(oasis_files))


class GenerateLossesCmd(OasisBaseCommand):
    """
    Generate losses using the installed ktools framework.

    Given Oasis files, model analysis settings JSON file, model data, and
    some other parameters. can generate losses using the installed ktools framework.

    The command line arguments can be supplied in the configuration file
    (``oasislmf.json`` by default or specified with the ``--config`` flag).
    Run ``oasislmf config --help`` for more information.

    The script creates a time-stamped folder in the model run directory and
    sets that as the new model run directory, copies the analysis settings
    JSON file into the run directory and creates the following folder
    structure
    ::

        ├── analysis_settings.json
        ├── fifo/
        ├── input/
        ├── output/
        ├── static/
        └── work/

    Depending on the OS type the model data is symlinked (Linux, Darwin) or
    copied (Cygwin, Windows) into the ``static`` subfolder. The input files
    are kept in the ``input`` subfolder and the losses are generated as CSV
    files in the ``output`` subfolder.

    By default executing the generated ktools losses script will automatically
    execute, this can be overridden by providing the ``--no-execute`` flag.
    """
    formatter_class = RawDescriptionHelpFormatter

    def add_args(self, parser):
        """
        Adds arguments to the argument parser.

        :param parser: The argument parser object
        :type parser: ArgumentParser
        """
        super(GenerateLossesCmd, self).add_args(parser)

        parser.add_argument('-o', '--oasis-files-path', default=None, help='Path to Oasis files')
        parser.add_argument('-j', '--analysis-settings-json-file-path', default=None, help='Relative or absolute path of the model analysis settings JSON file')
        parser.add_argument('-m', '--model-data-path', default=None, help='Model data source path')
        parser.add_argument('-r', '--model-run-dir-path', default=None, help='Model run directory path')
        parser.add_argument('-s', '--ktools-script-name', default=None, help='Relative or absolute path of the output file')
        parser.add_argument('-n', '--ktools-num-processes', default=-1, help='Number of ktools calculation processes to use')
        parser.add_argument('-x', '--no-execute', action='store_true', help='Whether to execute generated ktools script')

    def action(self, args):
        """
        Generate losses using the installed ktools framework.

        :param args: The arguments from the command line
        :type args: Namespace
        """
        inputs = InputValues(args)

        oasis_files_path = as_path(inputs.get('oasis_files_path', required=True, is_path=True), 'Oasis files', preexists=True)

        model_run_dir_path = as_path(inputs.get('model_run_dir_path', required=False, is_path=True), 'Model run directory', preexists=False)

        analysis_settings_json_file_path = as_path(
            inputs.get('analysis_settings_json_file_path', required=True, is_path=True),
            'Analysis settings file'
        )
        model_data_path = as_path(inputs.get('model_data_path', required=True, is_path=True), 'Model data')

        ktools_script_name = inputs.get('ktools_script_name', default='run_ktools')
        no_execute = inputs.get('no_execute', default=False)

        if not model_run_dir_path:
            utcnow = get_utctimestamp(fmt='%Y%m%d%H%M%S')
            model_run_dir_path = os.path.join(os.getcwd(), 'runs', 'ProgOasis-{}'.format(utcnow))
            self.logger.info('No model run dir. provided - creating a timestamped run dir. in working directory as {}'.format(model_run_dir_path))
            Path(model_run_dir_path).mkdir(parents=True, exist_ok=True)
        else:
            if not os.path.exists(model_run_dir_path):
                Path(model_run_dir_path).mkdir(parents=True, exist_ok=True)

        self.logger.info(
            'Preparing model run directory {} - copying Oasis files, analysis settings JSON file and linking model data'.format(model_run_dir_path)
        )
        prepare_model_run_directory(
            model_run_dir_path,
            oasis_files_path,
            analysis_settings_json_file_path,
            model_data_path
        )

        self.logger.info('Converting Oasis files to ktools binary files')
        oasis_files_path = os.path.join(model_run_dir_path, 'input', 'csv')
        binary_files_path = os.path.join(model_run_dir_path, 'input')
        create_binary_files(oasis_files_path, binary_files_path)

        analysis_settings_json_file_path = os.path.join(model_run_dir_path, 'analysis_settings.json')
        try:
            self.logger.info('Reading analysis settings JSON file')
            with io.open(analysis_settings_json_file_path, 'r', encoding='utf-8') as f:
                analysis_settings = json.load(f)
                if 'analysis_settings' in analysis_settings:
                    analysis_settings = analysis_settings['analysis_settings']
        except (IOError, TypeError, ValueError):
            raise OasisException('Invalid analysis settings JSON file or file path: {}.'.format(analysis_settings_json_file_path))

        self.logger.info('Loaded analysis settings JSON: {}'.format(analysis_settings))

        self.logger.info('Preparing model run inputs')
        prepare_model_run_inputs(analysis_settings, model_run_dir_path)

        script_path = os.path.join(model_run_dir_path, '{}.sh'.format(ktools_script_name))
        if no_execute:
            self.logger.info('Generating ktools losses script')
            genbash(
                args.ktools_num_processes,
                analysis_settings,
                filename=script_path,
            )
            self.logger.info('Making ktools losses script executable')
            subprocess.check_call("chmod +x {}".format(script_path), stderr=subprocess.STDOUT, shell=True)
        else:
            os.chdir(model_run_dir_path)
            run(analysis_settings, args.ktools_num_processes, filename=script_path)

        self.logger.info('Loss outputs generated in {}'.format(os.path.join(model_run_dir_path, 'output')))


class RunCmd(OasisBaseCommand):
    """
    Run models end to end.

    The command line arguments can be supplied in the configuration file
    (``oasislmf.json`` by default or specified with the ``--config`` flag).
    """
    def add_args(self, parser):
        """
        Run models end to end.

        :param parser: The argument parser object
        :type parser: ArgumentParser
        """
        super(RunCmd, self).add_args(parser)

        parser.add_argument('-k', '--keys-data-path', default=None, help='Path to Oasis files')
        parser.add_argument('-v', '--model-version-file-path', default=None, help='Model version file path')
        parser.add_argument('-l', '--lookup-package-file-path', default=None, help='Keys data directory path')
        parser.add_argument(
            '-p', '--canonical-exposures-profile-json-path', default=None,
            help='Path of the supplier canonical exposures profile JSON file'
        )
        parser.add_argument('-e', '--source-exposures-file-path', default=None, help='Source exposures file path')
        parser.add_argument(
            '-a', '--source-exposures-validation-file-path', default=None,
            help='Source exposures validation file (XSD) path'
        )
        parser.add_argument(
            '-b', '--source-to-canonical-exposures-transformation-file-path', default=None,
            help='Source -> canonical exposures transformation file (XSLT) path'
        )
        parser.add_argument(
            '-c', '--canonical-exposures-validation-file-path', default=None,
            help='Canonical exposures validation file (XSD) path'
        )
        parser.add_argument(
            '-d', '--canonical-to-model-exposures-transformation-file-path', default=None,
            help='Canonical exposures validation file (XSD) path'
        )
        parser.add_argument(
            '-j', '--analysis-settings-json-file-path', default=None,
            help='Model analysis settings JSON file path'
        )
        parser.add_argument('-m', '--model-data-path', default=None, help='Model data source path')
        parser.add_argument('-r', '--model-run-dir-path', default=None, help='Model run directory path')
        parser.add_argument(
            '-s', '--ktools-script-name', default=None,
            help='Name of the ktools output script (should not contain any filetype extension)'
        )
        parser.add_argument('-n', '--ktools-num-processes', default=2, help='Number of ktools calculation processes to use')

    def action(self, args):
        """
        Generate Oasis files (items, coverages, GUL summary) for a model

        :param args: The arguments from the command line
        :type args: Namespace
        """
        inputs = InputValues(args)
        model_run_dir_path = as_path(inputs.get('model_run_dir_path', required=False), 'Model run path', preexists=False)

        if not model_run_dir_path:
            utcnow = get_utctimestamp(fmt='%Y%m%d%H%M%S')
            model_run_dir_path = os.path.join(os.getcwd(), 'runs', 'ProgOasis-{}'.format(utcnow))
            self.logger.info('No model run dir. provided - creating a timestamped run dir. in working directory as {}'.format(model_run_dir_path))
            Path(model_run_dir_path).mkdir(parents=True, exist_ok=True)
        else:
            if not os.path.exists(model_run_dir_path):
                Path(model_run_dir_path).mkdir(parents=True, exist_ok=True)

        args.model_run_dir_path = model_run_dir_path

        args.oasis_files_path = os.path.join(model_run_dir_path, 'tmp')
        self.logger.info('Creating temporary folder {} for Oasis files'.format(args.oasis_files_path))
        Path(args.oasis_files_path).mkdir(parents=True, exist_ok=True)

        gen_oasis_files_cmd = GenerateOasisFilesCmd()
        gen_oasis_files_cmd._logger = self.logger
        gen_oasis_files_cmd.action(args)

        gen_losses_cmd = GenerateLossesCmd()
        gen_losses_cmd._logger = self.logger
        gen_losses_cmd.action(args)


class CreateOrganisationCmd(OasisBaseCommand):
    def add_args(self, parser):
        parser.add_argument('organization_name', help='The name of the organization to create')
        parser.add_argument('model_name', help='The name of the first model to create')
        parser.add_argument(
            '--organization-slug', default=None,
            help='The slug to use for the organization (defaults to a slugified version of the organization name)'
        )
        parser.add_argument(
            '--model-slug', default=None,
            help='The slug to use for the model (defaults to a slugified version of the model name)'
        )
        parser.add_argument('--model-description', default=None, help='Short description of the model')
        parser.add_argument('--model-maintainer', default=None, help='The name of the primary model maintainer')
        parser.add_argument('--model-maintainer-email', default=None, help='The email of the primary model maintainer')
        parser.add_argument('--model-identifier', default=None, help='Short identifier for the model', type=slug)
        parser.add_argument('--model-version', default=None, help='The model version')
        parser.add_argument('--model-type', default=None, help='The type of model to use model version', choices=['Earthquake', 'Flood', 'Windstorm'])

    def run_cookiecutter(self, args, out_path):
        args.organization_slug = args.organization_slug or slug(args.organization_name)
        args.model_slug = args.model_slug or slug(args.model_name)
        args.model_identifier = args.model_identifier or ''.join(part[0].upper() for part in args.model_name.split())

        template_dir = Path(__file__).parent.parent.joinpath('_data', 'model-cc-template')

        context_file = template_dir.joinpath('cookiecutter.json')

        args_context = {
            'organization': args.organization_name,
            'organization_slug': args.organization_slug,
            'project_name': args.model_name,
            'project_slug': args.model_slug,
            'model_identifier': args.model_identifier,
        }
        defaults = {}

        if args.model_description:
            args_context['project_short_description'] = args.model_description

        if args.model_maintainer:
            args_context['project_maintainer'] = args.model_maintainer
        else:
            res = subprocess.run(['git', 'config', '--global', 'user.name'], stdout=subprocess.PIPE)
            uname = res.stdout.strip().decode()
            if res and uname:
                defaults['project_maintainer'] = uname

        if args.model_maintainer_email:
            args_context['project_maintainer_email'] = args.model_maintainer_email
        else:
            res = subprocess.run(['git', 'config', '--global', 'user.email'], stdout=subprocess.PIPE)
            email = res.stdout.strip().decode()
            if res and email:
                defaults['project_maintainer_email'] = email

        if args.model_version:
            args_context['model_version'] = args.model_version

        if args.model_type:
            args_context['model_type'] = args.model_type

        # setup the context file stripping any defined values
        with open(str(context_file)) as f:
            defaults.update({
                k: v for k, v in json.load(f).items() if k not in args_context and k not in defaults
            })

        with tempfile.NamedTemporaryFile('w') as cc_conf:
            json.dump(defaults, cc_conf)
            cc_conf.flush()

            context = {
                'cookiecutter': generate_context(
                    context_file=cc_conf.name,
                    extra_context=args_context,
                )[os.path.basename(cc_conf.name)]
            }
            context['cookiecutter'].update(prompt_for_config(context))
            context['cookiecutter'].update(args_context)
            context['cookiecutter']['_template'] = str(template_dir)

        generate_files(
            repo_dir=str(template_dir),
            context=context,
            overwrite_if_exists=False,
            output_dir=str(out_path)
        )

        self.logger.info(
            'You now need to add your model specific data to the model and edit "canonical-profile.json", '
            '"analysis-settings.json" and "oasislmf.json" files to match your specific model.'
        )

        return context['cookiecutter']

    def setup_git(self, cc_context, out_path):
        pwd = os.getcwd()
        try:
            os.chdir(str(out_path))
            subprocess.check_call(['git', 'init'])
            subprocess.check_call(['git', 'add', '.'])
            subprocess.check_call(['git', 'commit', '-m', 'Post-project creation initialisation'])

            subprocess.check_call(['git', 'config', 'user.name', cc_context['project_maintainer']])
            subprocess.check_call(['git', 'config', 'user.email', cc_context['project_maintainer_email']])
        finally:
            os.chdir(pwd)

    def action(self, args):
        organization_path = Path(slug(args.organization_name))
        if organization_path.exists():
            self.logger.error('The supplier at {} already exists'.format(organization_path))
            return 1

        organization_path.mkdir(parents=True)

        cc_context = self.run_cookiecutter(args, organization_path)
        self.setup_git(cc_context, organization_path)


class ModelsCmd(OasisBaseCommand):
    sub_commands = {
        'generate-keys': GenerateKeysCmd,
        'generate-oasis-files': GenerateOasisFilesCmd,
        'generate-losses': GenerateLossesCmd,
        'run': RunCmd,
        'create-organization': CreateOrganisationCmd,
    }
