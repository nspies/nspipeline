import os
from nspipeline.utilities import pickle

from nspipeline import reference
from nspipeline import utilities
from nspipeline import jobmanagers

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

    @classmethod
    def deserialize(cls, options_dict, options_path):
        options = cls(options_path)

        options.cluster_settings = ClusterSettings.deserialize(
            options_dict.pop("cluster_settings", {}))
        options.__dict__.update(options_dict)

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
        except (pickle.PicklingError, TypeError):
            print("Error pickling options -- couldn't serialize all objects! please ")
            print("try overriding __getstate__()")
            raise
            
        return state


    def get_jobmanager(self, processes=None):
        #def make_jobmanager(jobmanager_settings, processes, batch_dir):

        if processes is None:
            processes = self.cluster_settings.processes

        jobmanager_settings = self.cluster_settings
        batch_dir = self.working_dir
            
        jobmanager_classes = {"IPCluster":jobmanagers.IPCluster,
                              "local":    jobmanagers.LocalCluster,
                              "multiprocessing": jobmanagers.MultiprocessingCluster,
                              "admiral": jobmanagers.AdmiralCluster}
        
        cls = jobmanager_classes[jobmanager_settings.cluster_type]
        return cls(processes, jobmanager_settings, batch_dir)




class OptionsWithReference(Options):
    def __init__(self, options_path, debug=False):
        super(OptionsWithReference, self).__init__(options_path, debug)
        self._reference = None

    @property
    def reference(self):
        if self._reference is None:
            self._reference = reference.Reference(self.ref_fasta, self.debug)
        return self._reference

    def __getstate__(self):
        """
        allows pickling of Options instances, necessary for ipyparallel
        """
        state = self.__dict__.copy()
        state["_reference"] = None

        return state


def make_options(output_dir, ref_fasta=None, cluster_settings=None):
    if not output_dir.endswith("/"):
        output_dir += "/"

    options_dict = {}
    if cluster_settings is not None:
        options_dict["cluster_settings"] = cluster_settings

    if ref_fasta is not None:
        options_dict["ref_fasta"] = ref_fasta
        options = OptionsWithReference.deserialize(options_dict, output_dir)
    else:
        options = Options.deserialize(options_dict, output_dir)

    return options
