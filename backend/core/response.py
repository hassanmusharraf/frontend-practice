from rest_framework.response import Response


class StandardResponse(Response):
    """
    Custom response class to wrap data in a standard response format:
    {
        'success': bool,
        'message': str,
        'data': any,
        'errors': list,
        'has_notification': bool | None
    }
    """

    def __init__(self,
                 success=True,
                 message="",
                 data=None,
                 count=None,
                 errors=None,
                 has_notification=None,
                 status=None,
                 template_name=None,
                 headers=None,
                 exception=False,
                 content_type=None):
        if errors is None:
            errors = []

        response_data = {
            'success': success,
            'message': message,
            'data': data,
            'count': count,
            'errors': errors,
            'has_notification': has_notification
        }

        super().__init__(data=response_data,
                         status=status,
                         template_name=template_name,
                         headers=headers,
                         exception=exception,
                         content_type=content_type)



## Result to use in services to return and catch errors and data
class ServiceError(Exception):
    """Base exception for service-level errors."""
    def __init__(self, error, status=400, success=False):
        self.error = error
        self.status = status
        self.success = success
        super().__init__(error)