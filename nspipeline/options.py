import os
import cPickle as pickle

from nspipeline import utilities


class ClusterSettings(object):
    def __init__(self):
        self.cluster_type = "local"
        self.processes = utilities.cpu_count_physical()
        self.cluster_options = {}

    @staticmethod
    def deserialize(options_dict):
        settings = ClusterSettings()

        if "processes" in options_dict:
            settings.processes = options_dict["processes"]
        if "cluster_type" in options_dict:
            settings.cluster_type = options_dict["cluster_type"]
        if "cluster_options" in options_dict:
            settings.cluster_options = options_dict["cluster_options"]

        return settings

    def serialize(self):
        return {
            "processes": self.processes,
            "cluster_type": self.cluster_type,
            "cluster_options": self.cluster_options
        }

    
class Options(object):
    """
    simple options class, provides only a wrapper around ClusterSettings;
    subclass this class and provide at least a deserialize option to add 
    custom settings
    """
    def __init__(self, options_path, debug=False):
        self.options_path = options_path
        self._output_dir = os.path.dirname(self.options_path)

        self.cluster_settings = ClusterSettings()

        self.debug = debug

    @staticmethod
    def deserialize(options_dict, options_path):
        options = Options(options_path)

        options.cluster_settings = ClusterSettings.deserialize(
            options_dict.get("cluster_settings", {}))

        return options

    @property
    def output_dir(self):
        return self._output_dir
    
    @property
    def results_dir(self):
        return os.path.join(self.output_dir, "results")

    @property
    def working_dir(self):
        return os.path.join(self.output_dir, "working")

    @property
    def log_dir(self):
        return os.path.join(self.output_dir, "logs")


    def __getstate__(self):
        """
        allows pickling of Options instances, necessary for ipyparallel
        """
        state = self.__dict__.copy()

        try:
            pickle.dumps(state)
        except:
            print("Error pickling options -- couldn't serialize all objects! please ")
            print("try overriding __getstate__()")
            raise
            
        return state


