# ---
# python_callable: main
# python_callable_partials:
#   py_task:
#       greeting: "{{env_switch(default='hey')}}"
# ---


def main(greeting):
    return greeting
