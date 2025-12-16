import jwt
from django.conf import settings
from accounts.models import User
from django.http import JsonResponse
from portal.choices import Role
import time
from django.utils.deprecation import MiddlewareMixin
# from .opensearch_client import client
from rest_framework.response import Response as DRFResponse
import json
# from .opensearch_client import client

class AuthMiddleware:
    MAX_FILE_SIZE = 25 * 1024 * 1024  # 25 MB
    
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        token = request.headers.get("Authorization")
        excluded_paths = getattr(settings, "EXCLUDED_PATHS", [
            "/admin",
            "/api/accounts/login",
            "/api/media",
            "/media",
            "/download-pdf",
            "/api/operations/purchase-order/bulk-create/",
            "/api/operations/purchase-order/bulk-update/",
            "/api/accounts/change-password/",
            "/api/portal/oracle-test/"
            ])
            
        if not any(request.path.startswith(ex_path) for ex_path in excluded_paths):
        # if request.path.startswith("/api") and not any(ex_path in request.path for ex_path in excluded_paths):
            if not token:
                return JsonResponse(data={"msg": "Token not provided"}, status=403)

            for file in request.FILES.values():
                if file.size > self.MAX_FILE_SIZE:
                    return JsonResponse({"errors": f"File '{file.name}' exceeds 25 MB limit."}, status=400)
              
            
            try:
                jwt_data = jwt.decode(
                    token,
                    settings.JWT_SECRET,
                    algorithms=[settings.JWT_ALGORITHM]
                )

                user_obj = User.objects.get(id=jwt_data.get("user_id"),is_active=True)
                request.this_user = user_obj
                request.has_notif = user_obj.has_notif
                
                # user_data = {"user_id": user_obj.id, "role": user_obj.role}
                
                # if user_obj.role == Role.SUPPLIER_USER:
                #     user_data.update({
                #         "supplier_id": user_obj.supplier_profile.id,
                #         "supplier_user_id": user_obj.supplier_profile.supplier.id
                #     })
                    
                # elif user_obj.role == Role.CLIENT_USER:
                #     user_data.update({
                #         "client_id": user_obj.client_profile.id,
                #         "client_user_id": user_obj.client_profile.client.id
                #     })
                    
                # elif user_obj.role == Role.OPERATIONS:
                #     user_data.update({
                #         "operations_id": user_obj.operations_profile.id,
                #     })

                # request.this_user_data = user_data 

                response = self.get_response(request)

                # Append has_notification to JSON responses when available
                try:
                    has_notification = bool(getattr(request, "has_notif", False))
                    if isinstance(response, DRFResponse):
                        if isinstance(response.data, dict):
                            response.data["has_notification"] = has_notification
                            # If already rendered by DRF, re-render to persist changes
                            if getattr(response, "is_rendered", False):
                                response._is_rendered = False
                                response.render()
                    elif isinstance(response, JsonResponse):
                        try:
                            payload = json.loads(response.content)
                            if isinstance(payload, dict):
                                payload["has_notification"] = has_notification
                                response.content = json.dumps(payload).encode("utf-8")
                                if "Content-Length" in response:
                                    response["Content-Length"] = str(len(response.content))
                        except Exception:
                            pass
                except Exception:
                    pass

                return response

            except (jwt.exceptions.InvalidSignatureError, User.DoesNotExist):
                return JsonResponse(data={"msg": "Invalid token"}, status=440)
            except jwt.exceptions.ExpiredSignatureError:
                return JsonResponse(data={"msg": "Session Expired"}, status=440)

        response = self.get_response(request)
        try:
            has_notification = bool(getattr(request, "has_notif", False))
            if isinstance(response, DRFResponse):
                if isinstance(response.data, dict):
                    response.data["has_notification"] = has_notification
                    if getattr(response, "is_rendered", False):
                        response._is_rendered = False
                        response.render()
            elif isinstance(response, JsonResponse):
                try:
                    payload = json.loads(response.content)
                    if isinstance(payload, dict):
                        payload["has_notification"] = has_notification
                        response.content = json.dumps(payload).encode("utf-8")
                        if "Content-Length" in response:
                            response["Content-Length"] = str(len(response.content))
                except Exception:
                    pass
        except Exception:
            pass
        return response







import time
import json
from django.utils.deprecation import MiddlewareMixin
from .opensearch_client import client

class OpenSearchLoggingMiddleware(MiddlewareMixin):
    INDEX = "django-api-logs"

    def process_request(self, request):
        request._start = time.time()
        try:
            raw = request.body               # this is still bytes
            request._body_text = raw.decode("utf-8")
        except Exception:
            request._body_text = ""

    def process_response(self, request, response):
        duration = (time.time() - getattr(request, "_start", time.time())) * 1000
        doc = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "method":    request.method,
            "path":      request.get_full_path(),
            "status":    response.status_code,
            "duration":  round(duration, 2),
            "user":      getattr(request.user, "username", None),
            "client_ip": request.META.get("REMOTE_ADDR"),
            "request": {
                # use the text you saved, not request._body
                "body": request._body_text,
            },
            "response": {
                "body": getattr(response, "content", b"")[:2000]
                                   .decode("utf-8", errors="ignore")
            }
        }
        try:
            client.index(index=self.INDEX, body=doc)
        except Exception as e:
            # this will now only fire on genuine connection/indexing errors
            print("OpenSearch log error:", e)
        return response
    




class SecurityHeadersMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
 
    def __call__(self, request):
        response = self.get_response(request)
 
        # Enforce HTTPS
        response["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"
 
        # Prevent MIME-sniffing
        response["X-Content-Type-Options"] = "nosniff"
 
        # Mitigate clickjacking
        response["X-Frame-Options"] = "DENY"
 
        # Reduce XSS risks
        response["Content-Security-Policy"] = "default-src 'self'"
 
        # Prevent referrer data leakage
        response["Referrer-Policy"] = "strict-origin-when-cross-origin"
 
        # Restrict access to sensitive browser features
        response["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
 
        return response