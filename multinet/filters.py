class ChangeOnly:
    def __init__(self):
        self.old_data: dict = {}

    def __call__(self, data, ppm_user):
        prev_data = self.old_data.get(ppm_user, {})
        self.old_data[ppm_user] = data
        transformed_data = dict(data)
        for key in prev_data:
            if key in transformed_data and transformed_data[key] == prev_data[key]:
                del transformed_data[key]
        return transformed_data


