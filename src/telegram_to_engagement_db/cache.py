from src.common.cache import Cache


class TelegramGroupSyncCache(Cache):
    def get_latest_group_min_id(self, group_cache_file_name):
        """
        Gets the latest seen message.id cache for the given group.

        :param group_cache_file_name: Name of file to fetch cached min_id from.
        :type group_cache_file_name: str
        :return: Cached latest message.id, or None if there is no cached value for this context.
        :rtype: int | None
        """
        return self.get_string(group_cache_file_name)


    def set_latest_group_min_id(self, group_cache_file_name, min_id):
        """
        Sets the latest seen message.id cache for the given post_id.

        :param group_cache_file_name: Name of file to fetch cached min_id from.
        :type group_cache_file_name: str
        :param min_id: Latest seen message.id for the given group_id.
        :type min_id: int
        """
        self.set_string(group_cache_file_name, str(min_id))