from pathlib import Path
from environ import environ
import os

env = environ.Env()
environ.Env.read_env()

BASE_DIR = Path(__file__).resolve().parent.parent
SECRET_KEY = env("DJANGO_SECRET_KEY")
JWT_SECRET = env("JWT_SECRET")
JWT_ALGORITHM = env("JWT_ALGORITHM")
DEBUG = env("DEBUG") == '1'
ALLOWED_HOSTS = env.list('ALLOWED_HOSTS', default=[])


INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

    'django_celery_beat',
    'corsheaders',
    'rest_framework',

    'accounts',
    'adhoc',
    'entities',
    'operations',
    'portal',
    'workflows',
]
MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    "corsheaders.middleware.CorsMiddleware",
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',

    "core.middlewares.AuthMiddleware",
    "crequest.middleware.CrequestMiddleware",
    
]

if not DEBUG:

    MIDDLEWARE += [
        "core.middlewares.SecurityHeadersMiddleware",
        "core.middlewares.OpenSearchLoggingMiddleware",
    ]


ROOT_URLCONF = 'core.urls'
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]
WSGI_APPLICATION = 'core.wsgi.application'

USE_MSSQL = env.bool("USE_MSSQL", default=False)

# Force version to 160 (SQL 2022) so old backends don't error on v17 --- JUST A STOP GAP ARRANGEMENT FOR NOW
try:
    from mssql.base import DatabaseWrapper as _DW
    _DW.sql_server_version = 160
except Exception:
    pass

if USE_MSSQL:
    extra_params = env.bool("MSSQL_EXTRA_PARAMS", default=False)
    
    db_options = {
        'driver': env('MSSQL_DRIVER', default='ODBC Driver 17 for SQL Server'),
    }

    # Only add extra_params if it's non-empty
    if extra_params:
        db_options['extra_params'] = 'Trusted_Connection=yes;'

    DATABASES = {
        'default': {
            'ENGINE': 'mssql',
            'NAME': env('MSSQL_DB_NAME'),
            'USER': env('MSSQL_DB_USER'),
            'PASSWORD': env('MSSQL_DB_PASSWORD'),
            'HOST': env('MSSQL_DB_HOST'),
            'PORT': env('MSSQL_DB_PORT'),
            'OPTIONS': db_options,
        }
    }
    # DATABASES = {
    #     'default': {
    #         'ENGINE': 'mssql',
    #         'NAME': env('MSSQL_DB_NAME', default='pickupdb'),
    #         'USER': env('MSSQL_DB_USER', default=''),
    #         'PASSWORD': env('MSSQL_DB_PASSWORD', default=''),
    #         'HOST': env('MSSQL_DB_HOST', default='Uk06LOGDB01VS'),
    #         'PORT': env('MSSQL_DB_PORT', default=''),
    #         'OPTIONS': {
    #             'driver': env('MSSQL_DRIVER', default='ODBC Driver 17 for SQL Server'),
    #             # 'extra_params': 'Trusted_Connection=yes;',
    #         },
    #     }
    # }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": env("DATABASE_NAME"),
            "USER": env("DATABASE_USER"),
            "PASSWORD": env("DATABASE_PASS"),
            "HOST": env("DATABASE_HOST"),
            "PORT": env("DATABASE_PORT"),
        }
    }
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'templates')],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',  # Enables admin navigation sidebar
                'django.contrib.auth.context_processors.auth',  # Required for admin authentication
                'django.contrib.messages.context_processors.messages',  # Required for admin messages
            ],
        },
    },
]

AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator', },
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator', },
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator', },
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator', },
]

CRON_CLASSES = [
    "operations.cron.StageConsignmentRemover",
]

AUTH_USER_MODEL = 'accounts.User'

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True

MEDIA_FOLDER_NAME = "media"
# MEDIA_URL = f"/api/{MEDIA_FOLDER_NAME}/"
MEDIA_URL = f"/{MEDIA_FOLDER_NAME}/"
MEDIA_ROOT = BASE_DIR.joinpath(f"{MEDIA_FOLDER_NAME}/")
STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles')
STATICFILES_DIRS = [os.path.join(BASE_DIR, 'static')]
# STATIC_URL = "/static/"
# STATICFILES_DIRS = [
#     BASE_DIR / "static"
# ]

# STATIC_ROOT = BASE_DIR / "staticfiles"
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
CORS_ALLOW_HEADERS = ["*",]
CORS_ORIGIN_ALLOW_ALL = True
CORS_ORIGINS_WHITELIST = []
CSRF_TRUSTED_ORIGINS = []
# LOGGING = {
#     "version": 1,
#     "filters": {
#         "require_debug_true": {
#             "()": "django.utils.log.RequireDebugTrue",
#         }
#     },
#     "handlers": {
#         "console": {
#             "level": "DEBUG",
#             "filters": ["require_debug_true"],
#             "class": "logging.StreamHandler",
#         }
#     },
#     "loggers": {
#         "django.db.backends": {
#             "level": "DEBUG",
#             "handlers": ["console"],
#         }
#     },
# }

if not DEBUG:
    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '{asctime} [{levelname}] {message}',
                'style': '{',
            },
        },
        # 'handlers': {
        #     'cron_file': {
        #         'level': 'INFO',
        #         'class': 'logging.FileHandler',
        #         'filename': os.path.join(BASE_DIR, 'logs/cron.log'),
        #         'formatter': 'simple',
        #     },
        # },
        # 'loggers': {
        #     'django_cron': {
        #         'handlers': ['cron_file'],
        #         'level': 'INFO',
        #         'propagate': False,
        #     },
        # },
    }

CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'

# Add this line to retain the existing behavior
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True

CELERY_BEAT_SCHEDULER = 'django_celery_beat.schedulers.DatabaseScheduler'