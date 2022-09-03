# ---
# python_callable: main
# op_kwargs:
#   foo: bar
#   min: max
#   outer_dict:
#       outer: dict
#   updated_dict:
#       updated: dict
# multi_task_spec:
#   nested_py:
#       op_kwargs:
#           biz: bazz
#           min: mouse
#           inner_dict:
#               inner: dict
#           updated_dict:
#               updated: updated
# ---


def main(foo, biz, min):
    return foo + biz
