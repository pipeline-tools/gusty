import ast, importlib.util
from absql import Runner as r


def render_frontmatter(frontmatter, runner=None, exclude=["sql"]):
    if runner is not None:
        renderable_frontmatter = {
            k: v for k, v in frontmatter.items() if k not in exclude
        }
        rendered_frontmatter = r.render_context(
            file_contents=renderable_frontmatter, extra_context=runner.extra_context
        )
        frontmatter.update(rendered_frontmatter)
    return frontmatter


def get_callable_from_file(file_path, callable_name):
    v = CallableFinder(callable_name)
    with open(file_path) as f:
        tree = ast.parse(f.read())
        v.visit(tree)
        if v.has_callable:
            mod_file = importlib.util.spec_from_file_location(
                "".join(i for i in file_path if i.isalnum()), file_path
            )
            mod = importlib.util.module_from_spec(mod_file)
            mod_file.loader.exec_module(mod)
            return getattr(mod, callable_name)
        else:
            assert (
                False
            ), "{file_path} specifies python_callable {callable} but {callable} not found in {file_path}".format(  # noqa
                file_path=file_path, callable=callable_name
            )


class CallableFinder(ast.NodeVisitor):
    def __init__(self, callable_name):
        self.has_callable = None
        self.callable_name = callable_name

    def visit_FunctionDef(self, node):
        ast.NodeVisitor.generic_visit(self, node)
        if node.name == self.callable_name:
            self.has_callable = True
