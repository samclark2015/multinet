from cad_io.cdev import tags


class AnyChange:
    """Filter for async requests  
    
    Filters out all values except those which have changed
    """
    def __init__(self):
        self.old_data: dict = {}

    def __call__(self, data, ppm_user):
        old_data = self.old_data.get(ppm_user, {})
        self.old_data[ppm_user] = data
        keys = [
            key
            for key in data.keys()
            if len(key) < 3
            or key[2]
            not in (
                tags.key_to_tag(tags.TIMESTAMP),
                tags.key_to_tag(tags.TIMESTAMPSECONDS_TAG),
                tags.key_to_tag(tags.TIMESTAMPNANOSECONDS_TAG),
            )
        ]
        same = all(key in old_data and old_data[key] == data[key] for key in keys)
        if not same:
            return data
        else:
            return {}
