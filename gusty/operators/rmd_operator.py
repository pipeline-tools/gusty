import os

from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.utils.decorators import apply_defaults

command_template = """
Rscript -e 'library(methods); rmarkdown::render("~/{basename}")' || exit 1; rm -f ~/{basename}; rm -f ~/{html_out}
"""

class RmdOperator(SSHOperator):
    """
    The RmdOperator executes the R Markdown file. Note that it is up to the Rmd itself
    to handle connecting to the database.
    """
    ui_color = "#75AADB"
    template_fields = SSHOperator.template_fields + ('file_path', )

    @apply_defaults
    def __init__(self, file_path, ssh_conn_id = "rserver_default", *args, **kwargs):
        self.file_path = file_path
        self.base_name = os.path.basename(self.file_path)
        self.html_output = self.base_name.replace('.Rmd', '.html')

        command = command_template.format(basename = self.base_name,
                                          html_out = self.html_output)

        super(RmdOperator, self).__init__(ssh_conn_id = ssh_conn_id, command = command,
                                          *args, **kwargs)

    def execute(self, context):
        # Run the command, but first put the file up on the server
        sftp_hook = SFTPHook(ftp_conn_id=self.ssh_conn_id,
                             timeout=self.timeout)

        sftp_hook.store_file(self.base_name, self.file_path)

        super(RmdOperator, self).execute(context)
