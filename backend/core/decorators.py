from functools import wraps
from typing import Iterable, Union, Any, Tuple

from portal.choices import Role, OperationUserRole
from core.response import StandardResponse


def _normalize_roles(roles: Iterable[Union[str, Role]]) -> set:

    normalized = set()
    normalized.add("Admin")
    for role in roles:
        if isinstance(role, Role):
            normalized.add(role.value)
        elif isinstance(role, OperationUserRole):
            normalized.add(role.value)
        else:
            normalized.add(str(role))
    return normalized


def role_required(*allowed_roles: Union[str, Role, OperationUserRole]) -> Any:
    """
    Decorator to restrict access to users with specific roles.

    Usage:
        @role_required(Role.ADMIN)
        def get(self, request):
            ...

        @role_required(Role.ADMIN, Role.OPERATIONS)
        def post(self, request):
            ...
    """
    allowed = _normalize_roles(allowed_roles)
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(*args: Tuple[Any, ...], **kwargs):
            # Support both function-based views and class-based view methods
            if args and hasattr(args[0], "META"):
                # function-based view: (request, ...)
                request = args[0]
            elif len(args) >= 2 and hasattr(args[1], "META"):
                # class-based view method: (self, request, ...)
                request = args[1]
            else:
                request = kwargs.get("request")

            user = getattr(request, "this_user", None) or getattr(request, "user", None)
            user_role = getattr(user, "role", None)

            if user_role == Role.OPERATIONS:
                user = user.profile()
                user_role = user.access_level

            if user is None or user_role is None:
                return StandardResponse(
                    status=403,
                    success=False,
                    message="Forbidden",
                    errors=["Authentication required or user role is unavailable"],
                )

            if user_role not in allowed:
                return StandardResponse(
                    status=403,
                    success=False,
                    message="Forbidden",
                    errors=["You do not have permission to perform this action."],
                )

            return view_func(*args, **kwargs)

        return _wrapped

    return decorator


