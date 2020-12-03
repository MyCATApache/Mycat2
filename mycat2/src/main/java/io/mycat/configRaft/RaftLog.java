package io.mycat.configRaft;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RaftLog {
    private static final Logger LOG = LoggerFactory.getLogger(RaftLog.class);

    private final String dataPath;
    private final String groupId;
    private final String serverIdStr;
    private final String initialConfStr;
    private final MycatPriorityElectionNode node;

    public RaftLog(
            final String dataPath,
            final String groupId,
            final String serverIdStr,
            final String initialConfStr
    ) {
        this(dataPath, groupId, serverIdStr, initialConfStr, null);
    }

    public RaftLog(
            final String dataPath,
            final String groupId,
            final String serverIdStr,
            final String initialConfStr,
            final MycatLeaderStateListener listener
    ) {
        this.dataPath = dataPath;
        this.groupId = groupId;
        this.serverIdStr = serverIdStr;
        this.initialConfStr = initialConfStr;

        this.node = createNode(dataPath, groupId, serverIdStr, initialConfStr,
                listener == null ? DEFAULT_LISTENER : listener);
    }

    public void addLearners(List<String> peerIdList, MycatTaskClosure closure) {
        Node node = this.node.getNode();
        List<PeerId> peerIds = peerIdList.stream().map(i -> PeerId.parsePeer(i)).collect(Collectors.toList());
        node.addLearners(peerIds, closure);
    }

    public void removeLearners(List<String> peerIdList, MycatTaskClosure closure) {
        Node node = this.node.getNode();
        List<PeerId> peerIds = peerIdList.stream().map(i -> PeerId.parsePeer(i)).collect(Collectors.toList());
        node.removeLearners(peerIds, closure);
    }

    public void removePeer(String peerIdText, MycatTaskClosure closure) {
        Node node = this.node.getNode();
        PeerId peerIds = PeerId.parsePeer(peerIdText);
        node.removePeer(peerIds, closure);
    }

    public void addPeer(String peerIdText, MycatTaskClosure closure) {
        Node node = this.node.getNode();
        PeerId peerIds = PeerId.parsePeer(peerIdText);
        node.addPeer(peerIds, closure);
    }

    public void get(String key, MycatTaskClosure closure) throws Exception {
        MycatPriorityElectionOnlyStateMachine fsm = node.getFsm();
        Object o = fsm.map.get(key);
        closure.setResponse(o);
        closure.setCmd("GET");
        closure.run(Status.OK());
    }

    public void set(String key, String value, MycatTaskClosure closure) throws JsonProcessingException {
        Task task = new Task();
        MycatEntry mycatEntry = new MycatEntry();
        mycatEntry.setCmd("SET");
        mycatEntry.setKey(key);
        mycatEntry.setValue(value);
        ObjectMapper objectMapper = new ObjectMapper();
        String s = objectMapper.writeValueAsString(mycatEntry);
        task.setData(ByteBuffer.wrap(s.getBytes()));
        task.setDone(closure);
        Node node = this.node.getNode();
        if (node.isLeader(true)) {
            node.apply(task);
        } else {
            Status not_leader = new Status(RaftError.EPERM, "Not leader");
            LOG.error(node.getLeaderId() + " " + not_leader.toString());
            closure.run(not_leader);
        }
    }

    public boolean isLeader() {
        return node.getNode().isLeader(true);
    }

    public static MycatPriorityElectionNode createNode(String dataPath,
                                                       String groupId,
                                                       String serverIdStr,
                                                       String initialConfStr,
                                                       MycatLeaderStateListener listener) {
        final MycatPriorityElectionNodeOptions priorityElectionOpts = new MycatPriorityElectionNodeOptions();
        priorityElectionOpts.setDataPath(dataPath);
        priorityElectionOpts.setGroupId(groupId);
        priorityElectionOpts.setServerAddress(serverIdStr);
        priorityElectionOpts.setInitialServerAddressList(initialConfStr);
        final MycatPriorityElectionNode node = new MycatPriorityElectionNode();
        node.addLeaderStateListener(listener);
        node.init(priorityElectionOpts);
        return node;
    }

    public final MycatLeaderStateListener DEFAULT_LISTENER = new MycatLeaderStateListener() {

        @Override
        public void onLeaderStart(long leaderTerm) {

            PeerId serverId = node.getNode().getLeaderId();
            int priority = serverId.getPriority();
            String ip = serverId.getIp();
            int port = serverId.getPort();

            System.out.println("[PriorityElectionBootstrap] Leader's ip is: " + ip + ", port: " + port
                    + ", priority: " + priority);
            System.out.println("[PriorityElectionBootstrap] Leader start on term: " + leaderTerm);
        }

        @Override
        public void onLeaderStop(long leaderTerm) {
            System.out.println("[PriorityElectionBootstrap] Leader stop on term: " + leaderTerm);
        }

        @Override
        public void onSet(String key, String value, Map<String, String> map) {

        }
    };
}
