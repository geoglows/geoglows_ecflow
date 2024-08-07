"""
Simple templating module.

The text of a template can contain embedded Python code blocks:

    <!statements!> -- statements between <! and !> are executed.
    <?expression?> -- the block is replaced with the expression value

Files can be included using

    <&includefile&> -- replaced with the contents of includefile

Variables can be passed into a template as keywords arguments
of the Template() constructor or Template.render() method.
These variables become local variables in the template.

for example, the following code:

    template = Template(
    '''\
    <?greetings?>,
    <!
    var = 123
    !>
    The var is <?var?>
    Goodbye!
    ''')

    output = template.render(greetings='Hello')
    sys.stdout.write(output)

will print:

    Hello,
    The var is 123
    Goodbye!

Modified by Aquaveo

Copyright 2024 ecmwf

Licensed under the Apache License, Version 2.0 (the "License")

http://www.apache.org/licenses/LICENSE-2.0
"""

import re as regex
import sys
from .py2 import unicode

class TemplateError(Exception):
    pass


class TemplateProcessor(object):

    def __init__(self,
            include_reader,
            lx='<!', rx='!>',
            le='<?', re='?>',
            li='<&', ri='&>',
            **env):
        """
        Create new template processor instance.

        Positional arguments:

          include_reader -- callable, should return text of include
                            (string) given the include name.

        Keyword arguments:

          lx -- left delimiter for exec blocks  (default: <!)
          rx -- right delimiter for exec blocks (default: !>)
          le -- left delimiter for eval blocks  (default: <?)
          re -- right delimiter for eval blocks (default: ?>)
          li -- left delimiter for include directives  (default: <&)
          ri -- right delimiter for include directives (default: &>)
          **env -- arbitrary keyword arguments; each becomes
                 a local variable in the template.

        """
        # escape some common regex special characters which
        # are also attractive for use in a delimiter: $,^,[,]
        # TODO: handle more special characters/sequences if needed...
        delims = [regex.sub(r'(\$|\^|\?|\[|\])', r'\\\g<1>', d
                  ) for d in [lx, rx, le, re, li, ri]]
        self._include_reader = include_reader
        self._pattern = '|'.join(delims) + '|.'
        self._tokens = {
            lx: 'lx',
            rx: 'rx',
            le: 'le',
            re: 're',
            li: 'li',
            ri: 'ri',
            'EOF': 'EOF'
            }
        self._env = env


    def process(self, template, **env):
        """
        Process the template and return processed string.

        Positional arguments:
        template -- template string

        Keyword arguments:
        **env -- arbitrary keyword arguments; each becomes
                 a local python variable in the template.
        """
        self._localenv = self._env.copy()
        self._localenv.update(env)
        return self._process(template)


    def _process(self, template, state='verbatim'):
        source = template.source
        lineno = 1
        output = ''
        expression = ''
        statements = ''
        include = ''
        globalenv = globals()
        localenv = self._localenv
        it = regex.finditer(
            self._pattern, unicode(template), flags=regex.DOTALL)

        while True:

            try:
                tokstr = next(it).group(0)
            except StopIteration:
                tokstr = 'EOF'
            token = self._tokens.get(tokstr, 'char')
            if tokstr == '\n':
                lineno += 1

            if state == 'verbatim':

                if token == 'char':
                    output += tokstr
                    continue
                if token == 'le':
                    state = 'eval'
                    continue
                if token == 'lx':
                    state = 'exec'
                    continue
                if token == 'li':
                    state = 'include'
                    continue
                if token == 'EOF':
                    break

            if state == 'eval':

                if token == 'char':
                    expression += tokstr
                    continue
                if token == 're':
                    try:
                        output += str(eval(expression, globalenv, localenv))
                    except Exception as e:
                        _raise_template_error(source, lineno, e)
                    expression = ''
                    state = 'verbatim'
                    continue

            if state == 'exec':

                if token == 'char':
                    statements += tokstr
                    continue
                if token == 'rx':
                    try:
                        exec(statements, globalenv, localenv)
                    except Exception as e:
                        _raise_template_error(source, lineno, e)
                    statements = ''
                    state = 'verbatim'
                    continue

            if state == 'include':

                if token == 'char':
                    include += tokstr
                    continue
                if token == 'ri':
                    include_name = include.strip()
                    try:
                        include_text = self._include_reader(include_name)
                    except Exception as e:
                        _raise_template_error(source, lineno, e)
                    processor = TemplateProcessor(
                            self._include_reader, **self._env)
                    output += processor(include_text)
                    include = ''
                    state = 'verbatim'
                    continue

            raise TemplateError(
                    '{}:{}: unexpected {}'.format(template.source, lineno, tokstr))
        return output


    def __call__(self, template, **env):
        return self.process(template, **env)


def _raise_template_error(source, lineno, e):
        # Python snippets embedded in a template can
        # raise any sorts of exceptions.
        # Best we can do is probably re-raise them, with
        # an additional information where in the template
        # the problem occured.
        raise e.__class__('{} line {}: {}'.format(source, lineno, str(e)))