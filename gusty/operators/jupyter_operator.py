import os

from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults

class JupyterOperator(BashOperator):
    """
    The JupyterOperator executes a Jupyter notebook file. Note that it is up to
    the ipynb itself to handle connecting to the database.
    """
    ui_color = "#ef8d50"

    @apply_defaults
    def __init__(self, file_path, **kwargs):
        self.ipynb_file = file_path
        self.html_output = file_path.replace('.ipynb', '.html')
        self.user = os.environ['EZ_AF_USER']
        self.pythonserver = os.environ['EZ_AF_PYTHON_SERVER']

        self.command = """(scp -o StrictHostKeyChecking=no {local_filepath} {user}@{pythonserver}:~/
                           ssh -o StrictHostKeyChecking=no {user}@{pythonserver} 'jupyter nbconvert --execute ~/{filepath_basename}' || exit 1;
                           ssh -o StrictHostKeyChecking=no {user}@{pythonserver} 'rm ~/{filepath_basename} && rm ~/{html_output_basename}';
                           )""".format(
                           user = self.user,
                           pythonserver = self.pythonserver,
                           local_filepath = self.ipynb_file,
                           filepath_basename = os.path.basename(self.ipynb_file),
                           html_output_basename = os.path.basename(self.html_output))

        super(JupyterOperator, self).__init__(
            bash_command = self.command,
            **kwargs)

