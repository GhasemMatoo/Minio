from minio import Minio


class ConnectionMinio:
    def __init__(self, access_key, secret_key, mino_server_url, bucket_name):
        self.access_key = access_key
        self.secret_key = secret_key
        self.mino_server_url = mino_server_url
        self.bucket_name = bucket_name
        self.client = None

    def connect_minio(self):
        try:
            if self.client is None:
                self.client = Minio(self.mino_server_url,
                                    access_key=self.access_key,
                                    secret_key=self.secret_key,
                                    secure=False)
                print("اتصال به MinIO با موفقیت انجام شد!")
            return self.client
        except Exception as e:
            raise e

    def create_bucket(self):
        self.client = self.connect_minio()
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)
            print(f'باکت "{self.bucket_name}" با موفقیت ایجاد شد!')
        else:
            print(f'باکت "{self.bucket_name}" از قبل وجود دارد.')
        return self.client

    def bucket_lists(self):
        return self.client.list_buckets()

    def list_objects_by_bucket(self, bucket_name=None, return_names=False):
        try:
            self.bucket_name = bucket_name if bucket_name else self.bucket_name
            if self.client.bucket_exists(self.bucket_name):
                objects = self.client.list_objects(self.bucket_name)
                if return_names:
                    objects_name_list = []
                    for obj in objects:
                        objects_name_list.append(obj.object_name)
                    return objects_name_list
                return objects
        except Exception as e:
            raise e

    def remove_objects_by_bucket(self, object_name, bucket_name=None):
        try:
            self.bucket_name = bucket_name if bucket_name else self.bucket_name
            if self.client.bucket_exists(self.bucket_name):
                self.client.remove_object(self.bucket_name, object_name)
                return True
        except Exception as e:
            return False

    def bucket_remove(self, bucket_name=None):
        try:
            self.bucket_name = bucket_name if bucket_name else self.bucket_name
            if self.client.bucket_exists(self.bucket_name):
                for obj in self.list_objects_by_bucket(self.bucket_name):
                    self.remove_objects_by_bucket(obj.object_name)
                self.client.remove_bucket(self.bucket_name)
                print(f'باکت "{self.bucket_name}" با موفقیت حذف شد.')
            else:
                print(f'باکت "{self.bucket_name}" وجود ندارد.')
        except Exception as e:
            raise e

    def upload_file(self, file_name, file_path, bucket_name=None):
        try:
            self.bucket_name = bucket_name if bucket_name else self.bucket_name
            self.client = self.create_bucket()
            self.client.fput_object(self.bucket_name, file_name, file_path)
            print(f'فایل "{file_name}" با موفقیت آپلود شد.')
        except Exception as e:
            raise e

    def download_file(self, file_name, file_path, bucket_name=None):
        try:
            self.bucket_name = bucket_name if bucket_name else self.bucket_name
            self.client.fget_object(bucket_name, file_name, file_path)
            print(f'فایل "{file_name}" با موفقیت دانلود شد به مسیر {file_path}')
        except Exception as e:
            raise e


class MinioAccessManager:
    def __init__(self, access_key, secret_key, mino_server_url, ):
        self.access_key = access_key
        self.secret_key = secret_key
        self.mino_server_url = mino_server_url
        self.client = None
        self.access_policy_rwd = {
            "policy_name": "policy_rwd",
            "policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:DeleteObject", "s3:GetObject", "s3:PutObject"],
                        "Resource": "arn:aws:s3:::my-bucket/*"
                    }
                ]
            }
        }

    def connect_minio(self):
        try:
            if self.client is None:
                self.client = Minio(self.mino_server_url,
                                    access_key=self.access_key,
                                    secret_key=self.secret_key,
                                    secure=False)
                print("اتصال به MinIO با موفقیت انجام شد!")
            return self.client
        except Exception as e:
            raise e

    def minio_add_user(self, username, password):
        try:
            if self.client is None:
                self.client = self.connect_minio()
            self.client.add_user(username, password)
            print(f'کاربر "{username}" با موفقیت ایجاد شد.')
        except Exception as e:
            raise e

    def minio_remove_user(self, username):
        try:
            if self.client is None:
                self.client = self.connect_minio()
            self.client.remove_user(username, )
            print(f'کاربر "{username}" با موفقیت حذف شد.')
        except Exception as e:
            raise e

    def minio_create_policy_rwd(self):
        try:
            if self.client is None:
                self.client = self.connect_minio()
                self.client.set_policy(
                    self.access_policy_rwd.get("policy_name"), self.access_policy_rwd.get("policy"))
                print(f'سیاست "{self.access_policy_rwd.get("policy_name")}" با موفقیت ایجاد شد.')
        except Exception as e:
            raise e

    def minio_add_policy_by_user(self, username, policy_name):
        try:
            if self.client is None:
                self.client = self.connect_minio()
            self.client.set_user_policy(username, policy_name)
            print(f'سیاست "{policy_name}" به کاربر "{username}" اختصاص داده شد.')
            return True
        except Exception as e:
            raise e
        


