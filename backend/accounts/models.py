from django.db import models
from django.contrib.auth.models import AbstractUser
from portal.choices import Role
from portal.base import BaseModel
from core.fields import MSSQLJSONField

class User(AbstractUser, BaseModel):
    role = models.CharField(max_length=20, choices=Role.choices)
    name = models.CharField(max_length=255)
    force_change_password = models.BooleanField(default=True)
    has_notif = models.BooleanField(default=False)
        
    def __str__(self):
        return self.name
    
    def profile(self):
        """Returns the user profile based on role."""
        if self.role == Role.SUPPLIER_USER:
            return self.supplier_profile
        elif self.role == Role.CLIENT_USER:
            return self.client_profile
        elif self.role == Role.OPERATIONS:
            return self.operations_profile
        return None
    
    class Meta:
        indexes = [
            models.Index(fields=['role'], name='user_role_idx'),
            models.Index(fields=['is_active', 'role'], name='user_active_role_idx'),
            models.Index(fields=['username'], name='user_username_idx'),  # For login queries
            models.Index(fields=['email'], name='user_email_idx'),  # For email lookups
        ]
    
class UserPreference(BaseModel):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name="preference")
    preference = MSSQLJSONField(default=dict)
    
    def __str__(self):
        return self.user.name
    
    
class RecentlySearch(BaseModel):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    purchase_order = models.TextField(default="[]")
    consignment = models.TextField(default="[]")
    pickup = models.TextField(default="[]")
    
    def __str__(self):
        return self.user.name