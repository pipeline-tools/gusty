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
