# ---
# email: new_email@gusty.com
# dependencies:
#   - direct_dep
# external_dependencies:
#   - a_whole_dag: all
# python_callable: hello_world
# ---


def hello_world():
    phrase = "hello python task"
    print(phrase)
    return phrase
