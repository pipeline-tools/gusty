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
#   nested_2:
#       op_kwargs:
#           updated_dict:
#               updated: unique
# ---


def main(foo, biz, min, outer_dict, inner_dict, updated_dict):
    return ", ".join(
        [
            foo,
            biz,
            min,
            outer_dict["outer"],
            inner_dict["inner"],
            updated_dict["updated"],
        ]
    )
