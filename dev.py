import inspect
from collections.abc import Mapping
from functools import partial, wraps
from inspect import _empty as insp_empty
from inspect import _ParameterKind as ParKind
from inspect import signature
from itertools import groupby
from types import NoneType
from typing import (
    Any,
    Callable,
    Concatenate,
    Dict,
    List,
    Optional,
    ParamSpec,
    TypeVar,
    Union,
)

import numpy as np
from matplotlib import pyplot as plt
from matplotlib.axes import Axes

P = ParamSpec("P")
T = TypeVar("T")
out_img = TypeVar("out_img", str, NoneType)


def combine_signatures(func, wrapper=None, include=None):
    """Adds keyword-only parameters from wrapper to signature

    Args:
      - func: The 'user' func that is being decorated and replaced by 'wrapper'
      - wrapper: The 'traditional' decorator which keyword-only parametrs should be added to the
            wrapped-function ('func')'s signature
      - include: optional list of keyword parameters that even not being present
            on the wrappers signature, will be included on the final signature.
            (if passed, these named arguments will be part of the kwargs)

    Use this in place of `functools.wraps`
    It works by creating a dummy function with the attrs of func, but with
    extra, KEYWORD_ONLY parameters from 'wrapper'.
    To be used in decorators that add new keyword parameters as
    the "__wrapped__"

    Usage:

    def decorator(func):
        @combine_signatures(func)
        def wrapper(*args, new_parameter=None, **kwargs):
            ...
            return func(*args, **kwargs)
        return wrapper
    """

    if wrapper is None:
        return partial(combine_signatures, func, include=include)

    sig_func = signature(func)
    sig_wrapper = signature(wrapper)
    pars_func = {group: list(params) for group, params in groupby(sig_func.parameters.values(), key=lambda p: p.kind)}
    pars_wrapper = {
        group: list(params) for group, params in groupby(sig_wrapper.parameters.values(), key=lambda p: p.kind)
    }

    def render_params(p):
        return f"{'=' + repr(p.default) if p.default != insp_empty else ''}"

    def render_by_kind(groups, key):
        parameters = groups.get(key, [])
        return [f"{p.name}{render_params(p)}" for p in parameters]

    pos_only = render_by_kind(pars_func, ParKind.POSITIONAL_ONLY)
    pos_or_keyword = render_by_kind(pars_func, ParKind.POSITIONAL_OR_KEYWORD)
    var_positional = [p for p in pars_func.get(ParKind.VAR_POSITIONAL, [])]
    keyword_only = render_by_kind(pars_func, ParKind.KEYWORD_ONLY)
    var_keyword = [p for p in pars_func.get(ParKind.VAR_KEYWORD, [])]

    extra_parameters = render_by_kind(pars_wrapper, ParKind.KEYWORD_ONLY)
    if include:
        if isinstance(include[0], Mapping):
            include = [
                f"{param['name']}{':' + param['annotation'] if 'annotation' in param else ''}{'=' + param['default'] if 'default' in param else ''}"
                for param in include
            ]
        else:
            include = [f"{name}=None" for name in include]

    def opt(seq, value=None):
        return ([value] if value else [", ".join(seq)]) if seq else []

    annotations = func.__annotations__.copy()
    for parameter in pars_wrapper.get(ParKind.KEYWORD_ONLY) or ():
        annotations[parameter.name] = parameter.annotation

    param_spec = ", ".join(
        [
            *opt(pos_only),
            *opt(pos_only, "/"),
            *opt(pos_or_keyword),
            *opt(
                keyword_only or extra_parameters,
                ("*" if not var_positional else f"*{var_positional[0].name}"),
            ),
            *opt(keyword_only),
            *opt(extra_parameters),
            *opt(include),
            *opt(var_keyword, f"**{var_keyword[0].name}" if var_keyword else ""),
        ]
    )

    coroutinedef = "async " if inspect.iscoroutinefunction(func) else ""
    declaration = f"{coroutinedef}def {func.__name__}({param_spec}): pass"

    f_globals = func.__globals__
    f_locals = {}

    exec(declaration, f_globals, f_locals)

    result = f_locals[func.__name__]
    result.__qualname__ = func.__qualname__
    result.__doc__ = func.__doc__
    result.__annotations__ = annotations

    defaults = [*func.__defaults__] + [eval(param.split("=")[-1]) for param in extra_parameters]
    result.__defaults__ = tuple(defaults)

    return wraps(result)(wrapper)

    # def copy_func(f, func_types, name=None):
    #     # add your code to first parameter
    #     new_func = types.FunctionType(f.__code__, f.__globals__, name or f.__name__,
    #         f.__defaults__, f.__closure__)
    #     new_func.__annotations__ = func_types
    #     return new_func

    # def template(arg):
    #      print('called template func')

    # a = copy_func(template, {'my_argument': int}, "test")
    # a(2) # can call it
    # print("types:", typing.get_type_hints(a)) # types: {'my_argument': <class 'type'>}


# If no type hint -> Pylance uses wrapper signature from the write signature (static)
# def generate_standalone_figure(drawing_func: Callable[..., None]) -> Callable[..., Optional[np.ndarray]]:
def generate_standalone_figure(
    drawing_func: Callable[P, T]
) -> Callable[Concatenate[out_img, P], Union[T, np.ndarray]]:
    @combine_signatures(drawing_func)
    def wrapper(
        *drawing_func_args: P.args,
        out_img: Optional[str] = None,
        **drawing_func_kwargs: P.kwargs,
    ) -> Union[T, np.ndarray]:
        fig, _ = plt.subplots()

        drawing_func(*drawing_func_args, **drawing_func_kwargs)

        fig.subplots_adjust(left=0.0, right=1.0, top=1.0, bottom=0.0)

        fig.savefig(out_img, bbox_inches="tight")

        plt.close()

    # new = combine_signatures(drawing_func, wrapper)

    return wrapper


def draw_kp_comparison(t: List[str] = ["test"], b: int = 2) -> None:
    """Draw test.

    :param t: Test variable.
    :type t: List[str]
    :return None: None.
    :rtype: None
    """

    ax = plt.gca()
    ax.plot(np.arange(10), np.arange(10))


@generate_standalone_figure
def decorated_draw_kp_comparison(t: List[str] = ["test"], b: int = 2) -> None:
    """Draw test.

    :param t: Test variable.
    :type t: List[str]
    :return None: None.
    :rtype: None
    """

    ax = plt.gca()
    ax.plot(np.arange(10), np.arange(10))


if __name__ == "__main__":
    # draw_kp_comparison(t=["l", "u"])
    # draw_kp_comparison()
    decorated_draw_kp_comparison(out_img=None)

    # from typing import Callable, TypeVar

    # from typing_extensions import Concatenate, ParamSpec

    # P = ParamSpec("P")
    # T = TypeVar("T")

    # def printing_decorator(func: Callable[P, T]) -> Callable[Concatenate[str, P], T]:
    #     def wrapper(msg: str, /, *args: P.args, **kwds: P.kwargs) -> T:
    #         print("Calling", func, "with", msg)
    #         return func(*args, **kwds)

    #     return wrapper

    # @printing_decorator
    # def add_forty_two(value: int) -> int:
    #     return value + 42

    # a = add_forty_two("three", 3)
