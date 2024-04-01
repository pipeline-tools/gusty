from airflow.models.baseoperator import BaseOperator
from gusty.building import _get_operator_parameters


def test_get_operator_parameters():
    class ACustomOperator(BaseOperator):
        template_fields = ("a_templated_field",)

        def __init__(self, a, **kwargs):
            self.a = 1

            super().__init__(**kwargs)

        def execute(self, context):
            print(self.a)

    params = _get_operator_parameters(ACustomOperator, {})

    assert "a" in params
    assert sorted(params) == ["a", "a_templated_field", "kwargs", "self"]


def test_get_operator_parameters_attribute():
    f = lambda a, **kwargs: BaseOperator(**kwargs)
    f._gusty_parameters = ("a",)

    params = _get_operator_parameters(f, {})

    assert "a" in params
    assert list(params) == ["a"]
