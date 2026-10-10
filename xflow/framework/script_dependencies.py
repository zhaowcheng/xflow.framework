# Copyright (c) 2026, zhaowcheng <zhaowcheng@163.com>

"""
静态收集共同脚本根目录内的 Shell 依赖，不执行脚本。

仅分析命令位置上的脚本引用；保留引号和替换表达式供静态求值。
"""
import re
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass(frozen=True)
class _Token:
    raw: str
    line: int
    operator: bool = False


def _quoted_end(text: str, start: int) -> int:
    """
    找到引号结尾，双引号内的命令替换保持完整。

    :param text: Shell 源码。
    :param start: 引号起点。
    :return: 引号后的下标。
    """
    quote = text[start]
    pos = start + 1
    while pos < len(text):
        if text[pos] == quote:
            return pos + 1
        if quote != "'" and text[pos] == '\\':
            pos += 2
        elif quote == '"' and text.startswith('$(', pos):
            pos = _substitution_end(text, pos)
        elif quote == '"' and text[pos] == '`':
            pos = _quoted_end(text, pos)
        else:
            pos += 1
    raise ValueError('unterminated quote')


def _substitution_end(text: str, start: int) -> int:
    """
    跳过完整命令替换或参数展开，不运行其中的内容。

    :param text: Shell 源码。
    :param start: 美元符号起点。
    :return: 表达式后的下标。
    """
    opening = text[start + 1]
    closing = ')' if opening == '(' else '}'
    depth = 1
    pos = start + 2
    while pos < len(text):
        char = text[pos]
        if char in "'\"`":
            pos = _quoted_end(text, pos)
        elif char == '\\':
            pos += 2
        elif text.startswith(('$(', '${'), pos):
            pos = _substitution_end(text, pos)
        else:
            if char == opening:
                depth += 1
            elif char == closing:
                depth -= 1
                if not depth:
                    return pos + 1
            pos += 1
    raise ValueError('unterminated substitution')


def _tokens(text: str) -> Iterator[_Token]:
    """
    分词并排除注释及 here-document 正文。

    :param text: Shell 源码。
    :return: 含原始行号的词和操作符。
    """
    pos = 0
    line = 1
    heredocs: list[tuple[str, bool]] = []
    delimiter_mode: bool | None = None
    while pos < len(text):
        char = text[pos]
        if text.startswith('\\\n', pos):
            pos += 2
            line += 1
            continue
        if char in ' \t\r':
            pos += 1
            continue
        if char == '#':
            end = text.find('\n', pos)
            pos = end if end >= 0 else len(text)
            continue
        if char == '\n':
            yield _Token('\n', line, True)
            pos += 1
            line += 1
            for delimiter, strip_tabs in heredocs:
                while pos < len(text):
                    end = text.find('\n', pos)
                    if end < 0:
                        end = len(text)
                    body = text[pos:end]
                    pos = min(end + 1, len(text))
                    line += 1
                    if (body.lstrip('\t') if strip_tabs else body) == delimiter:
                        break
                else:
                    raise ValueError('unterminated here-document')
            heredocs.clear()
            continue
        descriptor = re.match(r'\d+(?=[<>])', text[pos:])
        if descriptor:
            pos += len(descriptor[0])
            continue
        if char in ';|&()<>' or char in '{}' and (
            pos + 1 == len(text) or text[pos + 1].isspace() or text[pos + 1] in ';|&()'
        ):
            operator = char
            for candidate in ('((', '))', '<<-', '<<<', '<<', '>>', '&&', '||', ';;', ';&', '<&', '>&', '&>'):
                if text.startswith(candidate, pos):
                    operator = candidate
                    break
            yield _Token(operator, line, True)
            if operator in ('<<', '<<-'):
                delimiter_mode = operator == '<<-'
            pos += len(operator)
            continue
        start = pos
        word_line = line
        while pos < len(text):
            char = text[pos]
            if char.isspace() or char in ';|&()<>':
                break
            if char in "'\"`":
                pos = _quoted_end(text, pos)
            elif char == '\\':
                pos += 2
            elif text.startswith(('$(', '${'), pos):
                pos = _substitution_end(text, pos)
            else:
                pos += 1
        raw = text[start:pos]
        line += raw.count('\n')
        raw = raw.replace('\\\n', '')
        if delimiter_mode is not None:
            values = shlex.split(raw)
            if len(values) != 1:
                raise ValueError('invalid here-document delimiter')
            heredocs.append((values[0], delimiter_mode))
            delimiter_mode = None
        yield _Token(raw, word_line)


@dataclass(frozen=True)
class _Command:
    words: list[_Token]
    conditional: bool


def _commands(text: str) -> Iterator[_Command]:
    """
    收集简单命令，丢弃重定向目标，保留条件分支中的命令。

    :param text: Shell 源码。
    :return: 简单命令的词列表。
    """
    words: list[_Token] = []
    redirect = False
    depth = 0
    conditional = False
    conditional_next = False
    array_depth = 0
    arithmetic: list[_Token] | None = None
    case_patterns: list[bool] = []
    for token in _tokens(text):
        if array_depth:
            if token.operator:
                array_depth += token.raw.count('(') - token.raw.count(')')
            continue
        if arithmetic is not None:
            if token.raw == '))' and token.operator:
                names = set(re.findall(r'[A-Za-z_][A-Za-z_0-9]*', ' '.join(t.raw for t in arithmetic)))
                if names:
                    yield _Command([
                        _Token(f'{name}=$(unsupported arithmetic)', token.line)
                        for name in names
                    ], True)
                arithmetic = None
            else:
                arithmetic.append(token)
            continue
        if token.operator and token.raw == '((':
            arithmetic = []
            continue
        if token.operator and token.raw == '(' and words and re.fullmatch(
            r'[A-Za-z_][A-Za-z_0-9]*=', words[-1].raw
        ):
            words[-1] = _Token(words[-1].raw + '$(unsupported array)', words[-1].line)
            array_depth = 1
            continue
        if token.operator and token.raw == ')' and case_patterns and case_patterns[-1]:
            words = []
            case_patterns[-1] = False
            continue
        if token.operator and token.raw == ';;' and case_patterns:
            case_patterns[-1] = True
        if token.operator and ('<' in token.raw or '>' in token.raw):
            redirect = True
        elif redirect:
            redirect = False
        elif token.operator:
            if words:
                yield _Command(words, conditional or token.raw in ('|', '&'))
                words = []
                conditional_next = False
            if token.raw in ('&&', '||', '|'):
                conditional_next = True
            if token.raw in ('(', '{'):
                depth += 1
            elif token.raw in (')', '}'):
                depth = max(depth - 1, 0)
        else:
            if not words:
                if token.raw == 'case':
                    case_patterns.append(True)
                elif token.raw == 'esac' and case_patterns:
                    case_patterns.pop()
                if token.raw in ('if', 'while', 'until', 'for', 'select', 'case'):
                    depth += 1
                elif token.raw in ('fi', 'done', 'esac'):
                    depth = max(depth - 1, 0)
                conditional = depth > 0 or conditional_next
            words.append(token)
    if words:
        yield _Command(words, conditional)


def _simple_words(words: list[_Token]) -> list[_Token]:
    """
    去掉简单命令前的控制关键字。

    :param words: 命令词列表。
    :return: 简单命令本身的词列表。
    """
    words = words.copy()
    while words and words[0].raw in (
        'if', 'then', 'elif', 'else', 'while', 'until', 'do', '!'
    ):
        words.pop(0)
    return words


@dataclass(frozen=True)
class _Value:
    text: str
    local: bool = False
    tilde: bool = False


@dataclass(frozen=True)
class _Context:
    script: Path
    zero: Path


def _directory_expression(
    body: str,
    variables: dict[str, _Value | None],
    context: _Context
) -> _Value:
    """
    求值白名单中的 dirname 或 cd/dirname/pwd 组合。

    :param body: 命令替换正文。
    :param variables: 当前已知常量。
    :param context: 当前文件及 Shell 入口。
    :return: 带本地路径来源的目录。
    """
    tokens = list(_tokens(body.strip()))
    words = [token.raw for token in tokens]
    if words and words[0] == 'dirname':
        args = tokens[1:]
        if args and args[0].raw == '--':
            args = args[1:]
        if len(args) == 1 and not args[0].operator:
            value = _expand(args[0].raw, variables, context)
            if value.local:
                return _Value(str(Path(value.text).parent), True)
    if words and words[0] == 'CDPATH=':
        tokens = tokens[1:]
        words = words[1:]
    if words and words[0] == 'cd':
        args = tokens[1:]
        while args and args[0].raw in ('--', '-P', '-L'):
            args = args[1:]
        if len(args) in (3, 4) and args[1].raw == '&&' and args[2].raw == 'pwd':
            if len(args) == 3 or args[3].raw in ('-P', '-L'):
                value = _expand(args[0].raw, variables, context)
                if value.local:
                    return value
    raise ValueError('unresolved command substitution')


def _expand(
    raw: str,
    variables: dict[str, _Value | None],
    context: _Context,
    quoted: bool = False,
    split_words: bool = True
) -> _Value:
    """
    求值字面量和简单变量，未知值保留为明确错误。

    :param raw: 保留引号的 Shell 词。
    :param variables: 当前已知常量。
    :param context: 当前文件及 Shell 入口。
    :param quoted: 是否在双引号中。
    :param split_words: 是否检查未加引号的词拆分与通配符。
    :return: 求值结果及本地路径来源。
    """
    values: list[_Value] = []
    pos = 0
    while pos < len(raw):
        char = raw[pos]
        expansion = False
        if not quoted and char in "'\"":
            end = _quoted_end(raw, pos)
            if char == "'":
                value = _Value(raw[pos + 1:end - 1])
            else:
                value = _expand(raw[pos + 1:end - 1], variables, context, True)
        elif char == '\\':
            if pos + 1 >= len(raw):
                raise ValueError('unresolved trailing escape')
            following = raw[pos + 1]
            prefix = '\\' if quoted and following not in '$`"\\' else ''
            value = _Value(prefix + following)
            end = pos + 2
        elif char == '$':
            expansion = True
            if raw.startswith('$(', pos):
                end = _substitution_end(raw, pos)
                value = _directory_expression(
                    raw[pos + 2:end - 1], variables, context
                )
            else:
                if raw.startswith('${', pos):
                    end = _substitution_end(raw, pos)
                    name = raw[pos + 2:end - 1]
                else:
                    match = re.match(r'\$([A-Za-z_][A-Za-z_0-9]*|0)', raw[pos:])
                    if not match:
                        raise ValueError('unresolved parameter expansion')
                    name = match[1]
                    end = pos + len(match[0])
                if name in ('BASH_SOURCE', 'BASH_SOURCE[0]'):
                    value = _Value(str(context.script), True)
                elif name == '0':
                    value = _Value(str(context.zero), True)
                elif re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', name):
                    known = variables.get(name)
                    if known is None:
                        raise ValueError(f'unresolved variable: {name}')
                    value = known
                else:
                    raise ValueError(f'unresolved parameter expansion: {name}')
        elif char == '`':
            expansion = True
            end = _quoted_end(raw, pos)
            value = _directory_expression(
                raw[pos + 1:end - 1], variables, context
            )
        elif split_words and not quoted and char in '*?[{':
            raise ValueError('unresolved dynamic path')
        else:
            value = _Value(char)
            end = pos + 1
        if expansion and split_words and not quoted:
            if any(c.isspace() for c in value.text):
                raise ValueError('unresolved unquoted word splitting')
            if any(c in value.text for c in '*?['):
                raise ValueError('unresolved unquoted glob expansion')
        values.append(value)
        pos = end
    text = ''.join(value.text for value in values)
    local = any(value.local for value in values)
    tilde = text.startswith('~/') and (
        not quoted and raw.startswith('~/') or any(value.tilde for value in values)
    )
    return _Value(text, local, tilde)


@dataclass(frozen=True)
class _Reference:
    token: _Token
    sourced: bool = False


def _reference(
    words: list[_Token],
    variables: dict[str, _Value | None],
    context: _Context
) -> _Reference | None:
    """
    定位受支持引用形式的路径词。

    :param words: 简单命令的词列表。
    :param variables: 当前已知常量。
    :param context: 当前文件及 Shell 入口。
    :return: 脚本引用，或非脚本命令时返回空值。
    """
    words = _simple_words(words)
    while words and re.match(r'^[A-Za-z_][A-Za-z_0-9]*(\[[^]]*\])?\+?=', words[0].raw):
        words.pop(0)
    if not words:
        return None
    literal = shlex.split(words[0].raw)[0]
    if literal not in ('source', '.', 'bash', 'sh'):
        if '/' not in literal and '$' not in words[0].raw and '`' not in words[0].raw:
            return None
    command = _expand(words[0].raw, variables, context).text
    if command in ('source', '.'):
        index = 2 if len(words) > 1 and words[1].raw == '--' else 1
        if len(words) <= index:
            raise ValueError('missing source path')
        return _Reference(words[index], True)
    if command in ('bash', 'sh'):
        index = 1
        while index < len(words):
            option = shlex.split(words[index].raw)[0]
            if option == '--':
                index += 1
                break
            if not option.startswith(('-', '+')) or option in ('-', '+'):
                break
            if option.startswith('-') and not option.startswith('--'):
                if 'c' in option[1:] or 's' in option[1:]:
                    return None
            if option in ('-o', '+o', '-O', '+O'):
                index += 1
            elif option in ('--rcfile', '--init-file'):
                raise ValueError(f'unsupported interpreter option: {option}')
            index += 1
        return _Reference(words[index]) if index < len(words) else None
    if '/' in command:
        return _Reference(words[0])
    return None


def _assignments(
    command: _Command,
    variables: dict[str, _Value | None],
    context: _Context
) -> None:
    """
    记录无条件常量赋值；分支、循环及运行时赋值视为未知。

    :param command: 当前简单命令。
    :param variables: 要更新的常量表。
    :param context: 当前文件及 Shell 入口。
    """
    words = _simple_words(command.words)
    if not words:
        return
    head = words[0].raw
    if head == 'eval':
        variables.update({name: None for name in variables})
        return
    if head in ('read', 'unset', 'mapfile', 'readarray'):
        for word in words[1:]:
            if re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', word.raw):
                variables[word.raw] = None
        return
    if head == 'printf' and len(words) > 2 and words[1].raw == '-v':
        variables[words[2].raw] = None
        return
    if head in ('for', 'select') and len(words) > 1:
        variables[words[1].raw] = None
        return
    declaration = head in ('export', 'readonly', 'local', 'declare', 'typeset')
    unsupported = head in ('local', 'declare', 'typeset')
    assignment = r'^([A-Za-z_][A-Za-z_0-9]*)(\[[^]]*\])?(\+)?=(.*)$'
    if declaration:
        words = words[1:]
        unsupported |= any(w.raw.startswith('-') and w.raw != '--' for w in words)
    # 命令前的环境赋值不修改后续命令的 Shell 变量。
    elif any(not re.match(assignment, word.raw, re.S) for word in words):
        return
    for word in words:
        match = re.match(assignment, word.raw, re.S)
        if not match:
            if unsupported and re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', word.raw):
                variables[word.raw] = None
            continue
        name, subscript, append, raw = match.groups()
        if command.conditional or unsupported or subscript or append:
            variables[name] = None
            continue
        try:
            variables[name] = _expand(raw, variables, context, split_words=False)
        except ValueError:
            variables[name] = None


def collect_scripts(script: Path) -> list[Path]:
    """
    完整检查后返回需要交付的本地文件。

    :param script: 本地目标脚本的绝对路径。
    :return: 去重后的目标脚本和依赖脚本。
    """
    root = script.parent
    files: list[Path] = []
    active: list[Path] = []

    def visit(
        path: Path,
        variables: dict[str, _Value | None],
        zero: Path
    ) -> dict[str, _Value | None]:
        """
        检查文件及递归依赖，返回 source 后可用的常量。

        :param path: 本地脚本路径。
        :param variables: 当前已知常量。
        :param zero: 当前 Shell 的入口脚本。
        :return: 文件处理后的常量表。
        """
        if any(path.resolve() == ancestor.resolve() for ancestor in active):
            chain = ' -> '.join(str(p.relative_to(root)) for p in active + [path])
            raise ValueError(f'Script dependency cycle: {chain}')
        if not path.is_file():
            raise ValueError(f'{path}: missing script')
        active.append(path)
        if path not in files:
            files.append(path)
        variables = variables.copy()
        context = _Context(path, zero)
        try:
            text = path.read_text()
            if text.startswith('#!'):
                interpreter = shlex.split(text.splitlines()[0][2:])
                shell_names = ('sh', 'bash', 'dash', 'ash', 'ksh', 'zsh')
                if not any(Path(word).name in shell_names for word in interpreter):
                    active.pop()
                    return variables
            for command in _commands(text):
                _assignments(command, variables, context)
                try:
                    reference = _reference(command.words, variables, context)
                except ValueError as error:
                    line = command.words[0].line
                    raise ValueError(f'{path}:{line}: {error}') from None
                if reference is None:
                    continue
                token = reference.token
                try:
                    value = _expand(token.raw, variables, context)
                except ValueError as error:
                    raise ValueError(f'{path}:{token.line}: unresolved script path: {token.raw} ({error})') from None
                if (value.text.startswith('/') and not value.local) or value.tilde:
                    if reference.sourced:
                        variables.update({name: None for name in variables})
                    continue
                if '..' in Path(value.text).parts:
                    raise ValueError(f'{path}:{token.line}: parent references are unsupported: {value.text}')
                dependency = root / value.text
                if not dependency.resolve().is_relative_to(root.resolve()):
                    raise ValueError(f'{path}:{token.line}: dependency outside script root: {value.text}')
                if not value.text or not dependency.is_file():
                    raise ValueError(f'{path}:{token.line}: missing script dependency: {value.text}')
                sourced = reference.sourced
                result = visit(dependency, variables if sourced else {}, zero if sourced else dependency)
                if sourced:
                    if command.conditional:
                        for name, changed in result.items():
                            if changed != variables.get(name):
                                variables[name] = None
                    else:
                        variables = result
        except ValueError as error:
            if str(path) in str(error) or 'Script dependency cycle:' in str(error):
                raise
            raise ValueError(f'{path}: {error}') from None
        active.pop()
        return variables

    visit(script, {}, script)
    return files
