# ---
# python_callable: main
# op_kwargs:
#   foo: bar
#   min: max
# multi_task_spec:
#   nested_py:
#       op_kwargs:
#           biz: bazz
#           min: mouse
# ---


def main(foo, biz, min):
    return foo + biz
