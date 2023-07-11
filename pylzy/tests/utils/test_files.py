from lzy.utils.files import zip_module, fileobj_hash_str, fileobj_hash_bytes


def test_zip_module(tmp_path):
    f = tmp_path / 'file.txt'
    f.write_text('import os')

    ar = tmp_path / 'file.zip'
    ar.touch()

    with open(ar, 'rb') as ar_f:
        zip_module(f, ar_f)  # type: ignore[arg-type]
        # zipped content may differ depend on test env, so we just check zip file header
        assert ar_f.read()[:4] == b'PK\x03\x04'


def test_fileobj_hash(tmp_path):
    f = tmp_path / 'file.txt'
    f.write_bytes(b'123')
    with open(f, 'rb') as f_:
        assert fileobj_hash_str(f_) == '202cb962ac59075b964b07152d234b70'  # type: ignore[arg-type]
    with open(f, 'rb') as f_:
        assert fileobj_hash_bytes(f_) == b' ,\xb9b\xacY\x07[\x96K\x07\x15-#Kp'  # type: ignore[arg-type]
