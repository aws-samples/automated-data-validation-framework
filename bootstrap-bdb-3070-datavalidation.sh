set +e
set -x
echo "Starting the script"
path=/mnt/griffin
sudo su -l root -c "mkdir -p $path"
sudo su -l root -c "aws s3 sync s3://<bucket_name>/datavalidation/ $path"
sudo su -l root -c "chmod -R 757 $path"
