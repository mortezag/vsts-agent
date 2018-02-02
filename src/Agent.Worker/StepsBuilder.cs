using System;
using System.Collections.Generic;
using Pipelines = Microsoft.TeamFoundation.DistributedTask.Pipelines;
using System.Linq;

namespace Microsoft.VisualStudio.Services.Agent.Worker
{
    [ServiceLocator(Default = typeof(StepsBuilder))]
    public interface IStepsBuilder : IAgentService
    {
        List<IStep> Result { get; }
        void Build(IExecutionContext context, IList<Pipelines.JobStep> steps);
    }

    public class StepsBuilder : AgentService, IStepsBuilder
    {
        private readonly List<IStep> _result = new List<IStep>();

        Int32 _preInjectIndex = 0;
        Int32 _mainInjectIndex = 0;
        Int32 _postInjectIndex = 0;

        public List<IStep> Result => _result;

        public void AddPreStep(IStep step)
        {
            _result.Insert(_preInjectIndex, step);
            _preInjectIndex++;
            _mainInjectIndex++;
            _postInjectIndex++;
        }

        public void AddMainStep(IStep step)
        {
            _result.Insert(_mainInjectIndex, step);
            _mainInjectIndex++;
            _postInjectIndex++;
        }

        public void AddPostStep(IStep step)
        {
            _result.Insert(_postInjectIndex, step);
        }

        public void Build(IExecutionContext context, IList<Pipelines.JobStep> steps)
        {
            if (context.Container != null)
            {
                // Inject container create/stop steps to the jobSteps list.
                var containerProvider = HostContext.GetService<IContainerOperationProvider>();
                context.Debug($"Inject container start for '{context.Container.ContainerName}' at the front.");
                AddPreStep(containerProvider.GetContainerStartStep(context.Container));
                context.Debug($"Inject container stop for '{context.Container.ContainerName}' at the end.");
                AddPostStep(containerProvider.GetContainerStopStep(context.Container));
            }

            // Build up a basic list of steps for the job, expand warpper task
            var taskManager = HostContext.GetService<ITaskManager>();
            foreach (var task in steps.Where(x => x.Type == Pipelines.StepType.Task))
            {
                var taskLoadResult = taskManager.Load(context, task as Pipelines.TaskStep);

                // Add pre-job steps from Tasks
                if (taskLoadResult.PreScopeStep != null)
                {
                    Trace.Info($"Adding Pre-Job task step {task.DisplayName}.");
                    AddPreStep(taskLoadResult.PreScopeStep);
                }

                // Add execution steps from Tasks
                if (taskLoadResult.MainScopeStep != null)
                {
                    Trace.Verbose($"Adding task step {task.DisplayName}.");
                    AddMainStep(taskLoadResult.MainScopeStep);
                }

                // Add post-job steps from Tasks
                if (taskLoadResult.PostScopeStep != null)
                {
                    Trace.Verbose($"Adding Post-Job task step {task.DisplayName}.");
                    AddPostStep(taskLoadResult.PostScopeStep);
                }
            }
        }
    }
}