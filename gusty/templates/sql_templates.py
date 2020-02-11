from jinja2 import Template

postgres_create_table = Template('''
    -- Create table
    CREATE TABLE {{ schema }}.{{ task_id }}_tmp
    AS ({{ sql }});

    -- Drop existing table
    DROP TABLE IF EXISTS {{ schema }}.{{ task_id }};

    -- Replace query
    ALTER TABLE {{ schema }}.{{ task_id }}_tmp RENAME TO {{ task_id }};
''')

postgres_comment_table = Template('''
    -- Table description
    {% if description %}
        COMMENT ON TABLE {{ schema }}.{{ task_id }} IS '{{ description | replace("\'", "\'\'") }}';
    {% endif %}

    -- Field descriptions
    {% if fields %}
        {% for field in fields %}
            {% for field_name, field_description in field.items() %}
                COMMENT ON COLUMN {{ schema }}.{{ task_id }}."{{ field_name }}" IS '{{ field_description | replace("\'", "\'\'") }}';
            {% endfor %}
        {% endfor %}
    {% endif %}
''')
