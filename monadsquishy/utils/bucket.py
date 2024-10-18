import s3fs

class CustomS3filesystem(s3fs.S3FileSystem):

    async def _bulk_delete(self, pathlist, **kwargs):
        """
        Remove multiple keys with one call

        Parameters
        ----------
        pathlist : list(str)
            The keys to remove, must all be in the same bucket.
            Must have 0 < len <= 1000
        """
        if not pathlist:
            return []
        buckets = {self.split_path(path)[0] for path in pathlist}
        if len(buckets) > 1:
            raise ValueError("Bulk delete files should refer to only one bucket")
        bucket = buckets.pop()
        # if len(pathlist) > 1000:
        #     raise ValueError("Max number of files to delete in one call is 1000")
        delete_keys = {
            "Objects": [{"Key": self.split_path(path)[1]} for path in pathlist],
            "Quiet": True,
        }
        for path in pathlist:
            self.invalidate_cache(self._parent(path))

        # out = await self._call_s3(
        #     "delete_objects", kwargs, Bucket=bucket, Delete=delete_keys
        # )
        _delete_keys_ori = [elm.get('Key') for elm in delete_keys.get('Objects', [])]
        _delete_keys = [f"{bucket}/{elm}" for elm in _delete_keys_ori]
        out = {
            'Deleted': delete_keys.get('Objects', [])
        }
        _task = []
        for elm in _delete_keys:
            try:
                _task.append(self._rm_file(elm))
            except Exception as e:
                pass
        try:
            await asyncio.gather(*_task)
        except Exception as e:
            pass
        
        # TODO: we report on successes but don't raise on any errors, effectively
        #  on_error="omit"
        
        return [f"{bucket}/{_['Key']}" for _ in out.get("Deleted", [])]