dependencies = ["direct_dep"]

external_dependencies = [{"a_whole_dag": "all"}]

email = "new_email@gusty.com"


def python_callable():
    phrase = "hello python task"
    print(phrase)
    return phrase
