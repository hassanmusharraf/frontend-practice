from rest_framework.serializers import ModelSerializer
from .models import User


class UserPutSerializer(ModelSerializer):
    class Meta:
        model = User
        fields = "__all__"


class UserListSerializer(ModelSerializer):
    class Meta:
        model = User
        fields = ["id", "name", "username", "is_active", "role"]
        

class UserPostSerializer(ModelSerializer):
    class Meta:
        model = User
        fields = "__all__"

    def save(self):
        user = User(**self.validated_data)
        password = self.validated_data["password"]
        user.set_password(password)
        user.save()
        return user