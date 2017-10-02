package io.mycat.mycat2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.mycat.mycat2.beans.MySQLCharset;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.sqlannotations.AnnotationProcessor;
import io.mycat.mycat2.sqlannotations.SQLAnnotation;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mysql.AutoCommit;
import io.mycat.mysql.Isolation;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.AbstractSession;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.ParseUtil;
import io.mycat.util.StringUtil;

/**
 * 抽象的MySQL的连接会话
 * 
 * @author wuzhihui
 *
 */
public abstract class AbstractMySQLSession  extends AbstractSession {

	// 当前接收到的包类型
	public enum CurrPacketType {
		Full, LongHalfPacket, ShortHalfPacket
	}

	/**
	 * 字符集
	 */
	public MySQLCharset charSet = new MySQLCharset();
	/**
	 * 用户
	 */
	public String clientUser;

	/**
	 * 事务隔离级别
	 */
	public Isolation isolation = Isolation.REPEATED_READ;

	/**
	 * 事务提交方式
	 */
	public AutoCommit autoCommit = AutoCommit.ON;
	
	/**
	 * 认证中的seed报文数据
	 */
	public byte[] seed;
	
	//所有处理cmd中,用来向前段写数据,或者后端写数据的cmd的
	private CommandChain cmdChain = new CommandChain();
	
	public CommandChain getCmdChain(){
		if(cmdChain==null){
			cmdChain = new CommandChain();
			logger.warn(" curr session cmdChain is null.{}", this);
		}
		return cmdChain;
	}
	
	public void setCmdChain(CommandChain cmdChain){
		this.cmdChain = cmdChain;
	}
	
	/**
	 * 当前处理中的SQL报文的信息
	 */
	public MySQLPackageInf curMSQLPackgInf = new MySQLPackageInf();

	public AbstractMySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		this(bufferPool, selector, channel, SelectionKey.OP_READ);

	}

	public AbstractMySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel, int keyOpt)
			throws IOException {
		super(bufferPool, selector, channel, keyOpt);

	}

	public void setCurBufOwner(boolean curBufOwner) {
		this.curBufOwner = curBufOwner;
	}

	/**
	 * 回应客户端（front或Sever）OK 报文。
	 *
	 * @param pkg
	 *            ，必须要是OK报文或者Err报文
	 * @throws IOException
	 */
	public void responseOKOrError(MySQLPacket pkg) throws IOException {
		// proxyBuffer.changeOwner(true);
		this.proxyBuffer.reset();
		pkg.write(this.proxyBuffer);
		proxyBuffer.flip();
		proxyBuffer.readIndex = proxyBuffer.writeIndex;
		this.writeToChannel();
	}
	
	/**
	 * 回应客户端（front或Sever）OK 报文。
	 *
	 * @param pkg
	 *            ，必须要是OK报文或者Err报文
	 * @throws IOException
	 */
	public void responseOKOrError(byte[] pkg) throws IOException {
		// proxyBuffer.changeOwner(true);
		this.proxyBuffer.reset();
		proxyBuffer.writeBytes(OKPacket.OK);
		proxyBuffer.flip();
		proxyBuffer.readIndex = proxyBuffer.writeIndex;
		this.writeToChannel();
	}

	/**
	 * 解析MySQL报文，解析的结果存储在curMSQLPackgInf中，如果解析到完整的报文，就返回TRUE
	 * 如果解析的过程中同时要移动ProxyBuffer的readState位置，即标记为读过，后继调用开始解析下一个报文，则需要参数markReaded
	 * =true
	 *
	 * @param proxyBuf
	 * @return
	 * @throws IOException
	 */
	public CurrPacketType resolveMySQLPackage(ProxyBuffer proxyBuf, MySQLPackageInf curPackInf, boolean markReaded)
			throws IOException {

		ByteBuffer buffer = proxyBuf.getBuffer();
		// 读取的偏移位置
		int offset = proxyBuf.readIndex;
		// 读取的总长度
		int limit = proxyBuf.writeIndex;
		// 读取当前的总长度
		int totalLen = limit - offset;
		if (totalLen == 0) { // 透传情况下. 如果最后一个报文正好在buffer 最后位置,已经透传出去了.这里可能不会为零
			return CurrPacketType.ShortHalfPacket;
		}

		if (curPackInf.remainsBytes == 0 && curPackInf.crossBuffer) {
			curPackInf.crossBuffer = false;
		}

		// 如果当前跨多个报文
		if (curPackInf.crossBuffer) {
			if (curPackInf.remainsBytes <= totalLen) {
				// 剩余报文结束
				curPackInf.endPos = offset + curPackInf.remainsBytes;
				offset += curPackInf.remainsBytes; // 继续处理下一个报文
				proxyBuf.readIndex = offset;
				curPackInf.remainsBytes = 0;
			} else {// 剩余报文还没读完，等待下一次读取
				curPackInf.startPos = 0;
				curPackInf.remainsBytes -= totalLen;
				curPackInf.endPos = limit;
				proxyBuf.readIndex = curPackInf.endPos;
				return CurrPacketType.LongHalfPacket;
			}
		}
		// 验证当前指针位置是否
		if (!ParseUtil.validateHeader(offset, limit)) {
			// 收到短半包
			logger.debug("not read a whole packet ,session {},offset {} ,limit {}", getSessionId(), offset, limit);
			return CurrPacketType.ShortHalfPacket;
		}

		// 解包获取包的数据长度
		int pkgLength = ParseUtil.getPacketLength(buffer, offset);
		// 解析报文类型
		// final byte packetType = buffer.get(offset +
		// ParseUtil.msyql_packetHeaderSize);

		// 解析报文类型
		int packetType = -1;

		// 在包长度小于7时，作为resultSet的首包
		if (pkgLength <= 7) {
			int index = offset + ParseUtil.msyql_packetHeaderSize;

			long len = proxyBuf.getInt(index, 1) & 0xff;
			// 如果长度小于251,则取默认的长度
			if (len < 251) {
				packetType = (int) len;
			} else if (len == 0xfc) {
				// 进行验证是否位数足够,作为短包处理
				if (!ParseUtil.validateResultHeader(offset, limit, 2)) {
					// 收到短半包
					logger.debug("not read a whole packet ,session {},offset {} ,limit {}", getSessionId(), offset,
							limit);
					return CurrPacketType.ShortHalfPacket;
				}
				packetType = (int) proxyBuf.getInt(index + 1, 2);
			} else if (len == 0xfd) {

				// 进行验证是否位数足够,作为短包处理
				if (!ParseUtil.validateResultHeader(offset, limit, 3)) {
					// 收到短半包
					logger.debug("not read a whole packet ,session {},offset {} ,limit {}", getSessionId(), offset,
							limit);
					return CurrPacketType.ShortHalfPacket;
				}

				packetType = (int) proxyBuf.getInt(index + 1, 3);
			} else {
				// 进行验证是否位数足够,作为短包处理
				if (!ParseUtil.validateResultHeader(offset, limit, 8)) {
					// 收到短半包
					logger.debug("not read a whole packet ,session {},offset {} ,limit {}", getSessionId(), offset,
							limit);
					return CurrPacketType.ShortHalfPacket;
				}

				packetType = (int) proxyBuf.getInt(index + 1, 8);
			}
		} else {
			// 解析报文类型
			packetType = buffer.get(offset + ParseUtil.msyql_packetHeaderSize);
		}

		// 包的类型
		curPackInf.pkgType = packetType;
		// 设置包的长度
		curPackInf.pkgLength = pkgLength;
		// 设置偏移位置
		curPackInf.startPos = offset;

		curPackInf.crossBuffer = false;

		curPackInf.remainsBytes = 0;
		// 如果当前需要跨buffer处理
		if ((offset + pkgLength) > limit) {
			logger.debug("Not a whole packet: required length = {} bytes, cur total length = {} bytes, limit ={}, "
					+ "ready to handle the next read event", pkgLength, (limit - offset), limit);
			curPackInf.endPos = limit;
			return CurrPacketType.LongHalfPacket;
		} else {
			// 读到完整报文
			curPackInf.endPos = curPackInf.pkgLength + curPackInf.startPos;
			if (ProxyRuntime.INSTANCE.isTraceProtocol()) {
				/**
				 * @todo 跨多个报文的情况下，修正错误。
				 */
				final String hexs = StringUtil.dumpAsHex(buffer, curPackInf.startPos, curPackInf.pkgLength);
				logger.debug(
						"     session {} packet: startPos={}, offset = {}, length = {}, type = {}, cur total length = {},pkg HEX\r\n {}",
						getSessionId(), curPackInf.startPos, offset, pkgLength, packetType, limit, hexs);
			}
			if (markReaded) {
				proxyBuf.readIndex = curPackInf.endPos;
			}
			return CurrPacketType.Full;
		}
	}
	
	/**
	 * 命令处理链
	 * @author yanjunli
	 *
	 */
	public class CommandChain{
						
		/**
		 * 目标命令
		 */
		private MySQLCommand target;
		
		/**
		 * 本次匹配到的所有的动态注解，里面可能有重复的anno。但是是不同实例。可以根据自己的需要，选择去重复，或者不去重复。
		 */
		private List<SQLAnnotation> annontations = new ArrayList<>(30);
		
		/**
		 * queueMap 用于去重复
		 */
		private LinkedHashMap<Long,SQLAnnotation> queueMap = new LinkedHashMap<>(20);
		
		/**
		 * 前置类，后置类，around 类  动态注解  顺序，实现了SQLCommand 的动态注解会出现在此列表中
		 *  如果没有实现  SQLCommand 的 annotations 不会出现在此列表中
		 *  最终的构建结果
		 */		
		private List<SQLAnnotation> queue = new ArrayList<>(20);
		
		/**
		 * queue 列表当前索引值
		 */
		private int cmdIndex = 0;
		
		private String errMsg;
				
		public List<SQLAnnotation> getSqlAnnotations(){
			return annontations;
		}
		
		/**
		 * 查找当前命令的下一个处理命令
		 * @param pre
		 * @return
		 * @throws Exception
		 */
		public MySQLCommand getNextSQLCommand(){
			//sqlAnnotations 是  ArrayList .
			if(queue.isEmpty()|| ++cmdIndex >= queue.size()){
				/**
				 *  动态注解处理完成后，调用目标命令，同时重置 cmdIndex.
				 *  重置 cmdIndex  可以实现 对  around 类 动态注解的支持
				 */
				cmdIndex = 0;
				return target;
			}else{
				return queue.get(cmdIndex).getMySQLCommand();
			}
		}
		
		public Collection<SQLAnnotation> getSQLCommandsChain(){
			return queue;
		}
		
		public void build(){
			queue = queueMap.values().stream().collect(Collectors.toList());
		}
		
		/**
		 * 初始化设置  原始 命令
		 * @param target
		 */
		public CommandChain setTarget(MySQLCommand target){
			this.target = target;
			return this;
		}
		
		/**
		 * 处理动态注解和静态注解
		 * 动态注解 会覆盖静态注解
		 * @param session
		 */
		public CommandChain processAnnotation(MycatSession session,Map<Byte,SQLAnnotation> staticAnnontationMap){
			
			BufferSQLContext context = session.sqlContext;
			
			SQLAnnotation staticAnno = staticAnnontationMap.get(context.getAnnotationType());
			
			/**
			 * 处理动态注解
			 */
			List<SQLAnnotation> actions = session.getCmdChain().getSqlAnnotations();
			if(AnnotationProcessor.getInstance().parse(session.sqlContext, session, actions)){
				for(SQLAnnotation f:actions){
					if(!f.apply(session)){
						break;
					}
				}
			}
			
			if(staticAnno!=null){
				if(staticAnno.getMySQLCommand()!=null){
					/**
					 * 处理静态注解, 只有没有相应的动态注解时,才处理静态注解
					 */
					if(!session.getCmdChain().getSQLCommandsChain().contains(staticAnno)){
						session.getCmdChain().addCmdChain(staticAnno);
					}
				}else{
					staticAnno.apply(session);
				}
			}
			
			return this;
		}
		
		public MySQLCommand getCurrentSQLCommand(){
			//sqlAnnotations 是  ArrayList .
			if(queue.isEmpty()||cmdIndex >= queue.size()){
				return target;
			}else{
				return queue.get(cmdIndex).getMySQLCommand();
			}
		}
		
		public void addCmdChain(SQLAnnotation sqlanno){
			queueMap.put(sqlanno.currentKey(), sqlanno);
		}
		
		public void clear(){
			target = null;
			queue.clear();
			cmdIndex = 0;
			annontations.clear();
			queueMap.clear();
		}

		public String getErrMsg() {
			return errMsg;
		}

		public void setErrMsg(String errMsg) {
			this.errMsg = errMsg;
		}
	}

}
