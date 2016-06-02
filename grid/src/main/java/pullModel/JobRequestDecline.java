package pullModel;

public class JobRequestDecline {
    public int sourceId;
    public int numberOfJobs;
    public JobRequestDecline(int sourceId, int numberOfJobs) {
	this.sourceId = sourceId;
	this.numberOfJobs = numberOfJobs;
    }
}
