import multiprocessing

class Cluster(object):
    def start(self):
        pass
    def stop(self):
        pass

    def __init__(self, processes, cluster_settings, batch_dir):
        self.processes = processes
        self.cluster_settings = cluster_settings
        self.batch_dir = batch_dir


class LocalCluster(Cluster):
    def map(self, fn, args):
        # for arg in args:
        while len(args) > 0:
            arg = args.pop(0)
            fn(arg)


class IPCluster(Cluster):
    def __init__(self, processes, cluster_settings, batch_dir):
        super(IPCluster, self).__init__(processes, cluster_settings, batch_dir)
        self.view = None

    def start(self):
        if self.view is not None:
            print("using existing cluster view")
            return
        
        from cluster_helper.cluster import ClusterView

        cluster_args = {
            "scheduler": None,
            "queue": None,
            "num_jobs": self.processes,
            "extra_params": {"run_local": True}
        }

        cluster_args.update(self.cluster_settings.cluster_options)
        if not "local_controller" in cluster_args["extra_params"]:
            cluster_args["extra_params"]["local_controller"]
            
        print(cluster_args)

        self.view = ClusterView(**cluster_args)

    def stop(self):
        if self.view:
            self.view.stop()
            self.view = None
        
    def map(self, fn, args):
        async_results = self.view.view.map(fn, args, block=False)
        async_results.wait_interactive()
        return async_results.get()

class MultiprocessingCluster(Cluster):
    def map(self, fn, args):
        pool = multiprocessing.Pool(processes=self.processes)
        return pool.map_async(fn, args).get(999999)


class AdmiralCluster(Cluster):
    def map(self, fn, args):
        from admiral import jobmanagers, remote

        cluster_options = self.cluster_settings.cluster_options.copy()
        
        scheduler = cluster_options.pop("scheduler")

        jobmanager_class = jobmanagers.get_jobmanager(scheduler)
        jobmanager = jobmanager_class(
            batch_dir=self.batch_dir, log_dir=self.batch_dir)


        if not "mem" in cluster_options:
            cluster_options["mem"] = "16g"
        if not "time" in cluster_options:
            cluster_options["time"] = "12h"

        jobs = []
        #for i, arg in enumerate(args):

        job_name = args[0].__class__.__name__
        args = [[arg] for arg in args]
        job = remote.run_remote(fn, jobmanager, job_name, args=args,
                                array=True, overwrite=True, **cluster_options)

        result = jobmanagers.wait_for_jobs([job], wait=5, progress=True)

        if not result:
            raise Exception("Some chunks failed to complete")
