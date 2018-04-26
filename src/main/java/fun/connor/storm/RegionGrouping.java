package fun.connor.storm;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

public class RegionGrouping implements CustomStreamGrouping {
    private static final long serialVersionUID = 177788290277635963L;
    private static final Logger LOG = LoggerFactory.getLogger(RegionGrouping.class);
    private Integer[] taskList;

    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.taskList = new Integer[targetTasks.size()];
        for(int i = 0; i < targetTasks.size(); i++) taskList[i] = targetTasks.get(i);
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        // Just don't have a regionID greater than len(targetTasks). Just don't do it.
        Integer taskInt = taskList[Integer.parseInt((String) values.get(0))];

        LOG.info("RegionGrouping got values of: " + values.get(0) + " with taskInt of " + taskInt);

        List<Integer> taskSingle = new ArrayList<Integer>();
        taskSingle.add(taskInt);

        return taskSingle; // What a garbage lineo
    }
}