from minio import Minio


class ConnectionMinio:
    def __init__(self, access_key, secret_key, mino_server_url, bucket_name):
        self.access_key = access_key
        self.secret_key = secret_key
        self.mino_server_url = mino_server_url
        self.bucket_name = bucket_name

    def connect_minio(self):
        try:
            client = Minio(self.mino_server_url,
                           access_key=self.access_key,
                           secret_key=self.secret_key,
                           secure=False)
            print("اتصال به MinIO با موفقیت انجام شد!")
            return client
        except Exception as e:
            raise e

    def create_bucket(self):
        client = self.connect_minio()
        if not client.bucket_exists(self.bucket_name):
            client.make_bucket(self.bucket_name)
            print(f'باکت "{self.bucket_name}" با موفقیت ایجاد شد!')
        else:
            print(f'باکت "{self.bucket_name}" از قبل وجود دارد.')
        return client

    def bucket_lists(self):
        client = self.connect_minio()
        return client.list_buckets()

    def bucket_remove(self, bucket_name=None):
        try:
            self.bucket_name = bucket_name if bucket_name else self.bucket_name
            client = self.connect_minio()
            if client.bucket_exists(self.bucket_name):

                for obj in client.list_objects(self.bucket_name):
                    client.remove_object(self.bucket_name, obj.object_name)
                client.remove_bucket(self.bucket_name)
                print(f'باکت "{self.bucket_name}" با موفقیت حذف شد.')
            else:
                print(f'باکت "{self.bucket_name}" وجود ندارد.')
        except Exception as e:
            raise e

    def upload_file(self, file_name, file_path, bucket_name=None):
        try:
            self.bucket_name = bucket_name if bucket_name else self.bucket_name
            client = self.create_bucket()
            client.fput_object(self.bucket_name, file_name, file_path)
            print(f'فایل "{file_name}" با موفقیت آپلود شد.')
        except Exception as e:
            raise e


if __name__ == '__main__':
    access_key = "minioadmin"
    secret_key = "minioadmin"
    file_name = "t.text"
    file_path = "t.tex"
    mino_server_url = 'localhost:9000'
    bucket_name = "mybucket"
    mi = ConnectionMinio(access_key, secret_key, mino_server_url, bucket_name)
    mi.bucket_remove()
