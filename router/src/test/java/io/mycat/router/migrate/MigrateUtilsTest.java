package io.mycat.router.migrate;

import com.google.common.collect.Lists;
import io.mycat.router.NodeIndexRange;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;

import static io.mycat.router.migrate.MigrateUtils.merge;

/**
 * 迁移任务
 * Created by magicdoom on 2016/9/16.
 *
 * @author chenjunwen refactor
 */
public class MigrateUtilsTest {
    @Test
    public void balanceExpand() {
        List<List<NodeIndexRange>> integerListMap = new ArrayList<>();
        integerListMap.add(Lists.newArrayList(new NodeIndexRange(0, 0, 32)));
        integerListMap.add(Lists.newArrayList(new NodeIndexRange(1, 33, 65)));
        integerListMap.add(Lists.newArrayList(new NodeIndexRange(2, 66, 99)));
        int totalSlots = 100;
        List<String> oldDataNodes = Lists.newArrayList("dn1", "dn2", "dn3");
        List<String> newDataNodes = Lists.newArrayList("dn4", "dn5");


        SortedMap<String, List<MigrateTask>> tasks = MigrateUtils.balanceExpand(integerListMap, oldDataNodes, newDataNodes, totalSlots);
        Assert.assertEquals("{dn4=[MigrateTask{from='dn1'\n" +
                ", to='dn4'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=0, valueEnd=12)]\n" +
                "}, MigrateTask{from='dn2'\n" +
                ", to='dn4'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=33, valueEnd=39)]\n" +
                "}], dn5=[MigrateTask{from='dn2'\n" +
                ", to='dn5'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=40, valueEnd=45)]\n" +
                "}, MigrateTask{from='dn3'\n" +
                ", to='dn5'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=66, valueEnd=79)]\n" +
                "}]}", Objects.toString(tasks));

        merge(integerListMap, tasks);

        oldDataNodes = Lists.newArrayList("dn1", "dn2", "dn3", "dn4", "dn5");
        newDataNodes = Lists.newArrayList("dn6", "dn7", "dn8", "dn9");

        SortedMap<String, List<MigrateTask>> tasks1 = MigrateUtils.balanceExpand(integerListMap, oldDataNodes, newDataNodes, totalSlots);
        Assert.assertEquals("{dn6=[MigrateTask{from='dn1'\n" +
                ", to='dn6'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=13, valueEnd=21)]\n" +
                "}, MigrateTask{from='dn2'\n" +
                ", to='dn6'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=46, valueEnd=48)]\n" +
                "}], dn7=[MigrateTask{from='dn2'\n" +
                ", to='dn7'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=49, valueEnd=54)]\n" +
                "}, MigrateTask{from='dn3'\n" +
                ", to='dn7'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=80, valueEnd=84)]\n" +
                "}], dn8=[MigrateTask{from='dn3'\n" +
                ", to='dn8'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=85, valueEnd=88)]\n" +
                "}, MigrateTask{from='dn4'\n" +
                ", to='dn8'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=0, valueEnd=6)]\n" +
                "}], dn9=[MigrateTask{from='dn4'\n" +
                ", to='dn9'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=7, valueEnd=8)]\n" +
                "}, MigrateTask{from='dn5'\n" +
                ", to='dn9'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=40, valueEnd=45), NodeIndexRange(nodeIndex=2, valueStart=66, valueEnd=68)]\n" +
                "}]}", Objects.toString(tasks1));

        merge(integerListMap, tasks1);

        oldDataNodes = Lists.newArrayList("dn1", "dn2", "dn3", "dn4", "dn5", "dn6", "dn7", "dn8", "dn9");
        newDataNodes = Lists.newArrayList("dn10", "dn11", "dn12", "dn13", "dn14", "dn15", "dn16", "dn17", "dn18", "dn19", "dn20", "dn21", "dn22", "dn23", "dn24", "dn25", "dn26", "dn27", "dn28", "dn29", "dn30", "dn31", "dn32", "dn33", "dn34", "dn35", "dn36", "dn37", "dn38", "dn39", "dn40", "dn41", "dn42", "dn43", "dn44", "dn45", "dn46", "dn47", "dn48", "dn49", "dn50", "dn51", "dn52", "dn53", "dn54", "dn55", "dn56", "dn57", "dn58", "dn59", "dn60", "dn61", "dn62", "dn63", "dn64", "dn65", "dn66", "dn67", "dn68", "dn69", "dn70", "dn71", "dn72", "dn73", "dn74", "dn75", "dn76", "dn77", "dn78", "dn79", "dn80", "dn81", "dn82", "dn83", "dn84", "dn85", "dn86", "dn87", "dn88", "dn89", "dn90", "dn91", "dn92", "dn93", "dn94", "dn95", "dn96", "dn97", "dn98", "dn99", "dn100");
        SortedMap<String, List<MigrateTask>> tasks2 = MigrateUtils.balanceExpand(integerListMap, oldDataNodes, newDataNodes, totalSlots);
        Assert.assertEquals("{dn10=[MigrateTask{from='dn1'\n" +
                ", to='dn10'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=22, valueEnd=22)]\n" +
                "}], dn100=[MigrateTask{from='dn9'\n" +
                ", to='dn100'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=67, valueEnd=67)]\n" +
                "}], dn11=[MigrateTask{from='dn1'\n" +
                ", to='dn11'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=23, valueEnd=23)]\n" +
                "}], dn12=[MigrateTask{from='dn1'\n" +
                ", to='dn12'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=24, valueEnd=24)]\n" +
                "}], dn13=[MigrateTask{from='dn1'\n" +
                ", to='dn13'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=25, valueEnd=25)]\n" +
                "}], dn14=[MigrateTask{from='dn1'\n" +
                ", to='dn14'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=26, valueEnd=26)]\n" +
                "}], dn15=[MigrateTask{from='dn1'\n" +
                ", to='dn15'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=27, valueEnd=27)]\n" +
                "}], dn16=[MigrateTask{from='dn1'\n" +
                ", to='dn16'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=28, valueEnd=28)]\n" +
                "}], dn17=[MigrateTask{from='dn1'\n" +
                ", to='dn17'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=29, valueEnd=29)]\n" +
                "}], dn18=[MigrateTask{from='dn1'\n" +
                ", to='dn18'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=30, valueEnd=30)]\n" +
                "}], dn19=[MigrateTask{from='dn1'\n" +
                ", to='dn19'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=31, valueEnd=31)]\n" +
                "}], dn20=[MigrateTask{from='dn2'\n" +
                ", to='dn20'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=55, valueEnd=55)]\n" +
                "}], dn21=[MigrateTask{from='dn2'\n" +
                ", to='dn21'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=56, valueEnd=56)]\n" +
                "}], dn22=[MigrateTask{from='dn2'\n" +
                ", to='dn22'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=57, valueEnd=57)]\n" +
                "}], dn23=[MigrateTask{from='dn2'\n" +
                ", to='dn23'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=58, valueEnd=58)]\n" +
                "}], dn24=[MigrateTask{from='dn2'\n" +
                ", to='dn24'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=59, valueEnd=59)]\n" +
                "}], dn25=[MigrateTask{from='dn2'\n" +
                ", to='dn25'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=60, valueEnd=60)]\n" +
                "}], dn26=[MigrateTask{from='dn2'\n" +
                ", to='dn26'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=61, valueEnd=61)]\n" +
                "}], dn27=[MigrateTask{from='dn2'\n" +
                ", to='dn27'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=62, valueEnd=62)]\n" +
                "}], dn28=[MigrateTask{from='dn2'\n" +
                ", to='dn28'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=63, valueEnd=63)]\n" +
                "}], dn29=[MigrateTask{from='dn2'\n" +
                ", to='dn29'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=64, valueEnd=64)]\n" +
                "}], dn30=[MigrateTask{from='dn3'\n" +
                ", to='dn30'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=89, valueEnd=89)]\n" +
                "}], dn31=[MigrateTask{from='dn3'\n" +
                ", to='dn31'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=90, valueEnd=90)]\n" +
                "}], dn32=[MigrateTask{from='dn3'\n" +
                ", to='dn32'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=91, valueEnd=91)]\n" +
                "}], dn33=[MigrateTask{from='dn3'\n" +
                ", to='dn33'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=92, valueEnd=92)]\n" +
                "}], dn34=[MigrateTask{from='dn3'\n" +
                ", to='dn34'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=93, valueEnd=93)]\n" +
                "}], dn35=[MigrateTask{from='dn3'\n" +
                ", to='dn35'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=94, valueEnd=94)]\n" +
                "}], dn36=[MigrateTask{from='dn3'\n" +
                ", to='dn36'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=95, valueEnd=95)]\n" +
                "}], dn37=[MigrateTask{from='dn3'\n" +
                ", to='dn37'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=96, valueEnd=96)]\n" +
                "}], dn38=[MigrateTask{from='dn3'\n" +
                ", to='dn38'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=97, valueEnd=97)]\n" +
                "}], dn39=[MigrateTask{from='dn3'\n" +
                ", to='dn39'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=98, valueEnd=98)]\n" +
                "}], dn40=[MigrateTask{from='dn4'\n" +
                ", to='dn40'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=9, valueEnd=9)]\n" +
                "}], dn41=[MigrateTask{from='dn4'\n" +
                ", to='dn41'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=10, valueEnd=10)]\n" +
                "}], dn42=[MigrateTask{from='dn4'\n" +
                ", to='dn42'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=11, valueEnd=11)]\n" +
                "}], dn43=[MigrateTask{from='dn4'\n" +
                ", to='dn43'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=12, valueEnd=12)]\n" +
                "}], dn44=[MigrateTask{from='dn4'\n" +
                ", to='dn44'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=33, valueEnd=33)]\n" +
                "}], dn45=[MigrateTask{from='dn4'\n" +
                ", to='dn45'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=34, valueEnd=34)]\n" +
                "}], dn46=[MigrateTask{from='dn4'\n" +
                ", to='dn46'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=35, valueEnd=35)]\n" +
                "}], dn47=[MigrateTask{from='dn4'\n" +
                ", to='dn47'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=36, valueEnd=36)]\n" +
                "}], dn48=[MigrateTask{from='dn4'\n" +
                ", to='dn48'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=37, valueEnd=37)]\n" +
                "}], dn49=[MigrateTask{from='dn4'\n" +
                ", to='dn49'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=38, valueEnd=38)]\n" +
                "}], dn50=[MigrateTask{from='dn5'\n" +
                ", to='dn50'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=69, valueEnd=69)]\n" +
                "}], dn51=[MigrateTask{from='dn5'\n" +
                ", to='dn51'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=70, valueEnd=70)]\n" +
                "}], dn52=[MigrateTask{from='dn5'\n" +
                ", to='dn52'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=71, valueEnd=71)]\n" +
                "}], dn53=[MigrateTask{from='dn5'\n" +
                ", to='dn53'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=72, valueEnd=72)]\n" +
                "}], dn54=[MigrateTask{from='dn5'\n" +
                ", to='dn54'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=73, valueEnd=73)]\n" +
                "}], dn55=[MigrateTask{from='dn5'\n" +
                ", to='dn55'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=74, valueEnd=74)]\n" +
                "}], dn56=[MigrateTask{from='dn5'\n" +
                ", to='dn56'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=75, valueEnd=75)]\n" +
                "}], dn57=[MigrateTask{from='dn5'\n" +
                ", to='dn57'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=76, valueEnd=76)]\n" +
                "}], dn58=[MigrateTask{from='dn5'\n" +
                ", to='dn58'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=77, valueEnd=77)]\n" +
                "}], dn59=[MigrateTask{from='dn5'\n" +
                ", to='dn59'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=78, valueEnd=78)]\n" +
                "}], dn60=[MigrateTask{from='dn6'\n" +
                ", to='dn60'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=13, valueEnd=13)]\n" +
                "}], dn61=[MigrateTask{from='dn6'\n" +
                ", to='dn61'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=14, valueEnd=14)]\n" +
                "}], dn62=[MigrateTask{from='dn6'\n" +
                ", to='dn62'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=15, valueEnd=15)]\n" +
                "}], dn63=[MigrateTask{from='dn6'\n" +
                ", to='dn63'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=16, valueEnd=16)]\n" +
                "}], dn64=[MigrateTask{from='dn6'\n" +
                ", to='dn64'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=17, valueEnd=17)]\n" +
                "}], dn65=[MigrateTask{from='dn6'\n" +
                ", to='dn65'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=18, valueEnd=18)]\n" +
                "}], dn66=[MigrateTask{from='dn6'\n" +
                ", to='dn66'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=19, valueEnd=19)]\n" +
                "}], dn67=[MigrateTask{from='dn6'\n" +
                ", to='dn67'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=20, valueEnd=20)]\n" +
                "}], dn68=[MigrateTask{from='dn6'\n" +
                ", to='dn68'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=21, valueEnd=21)]\n" +
                "}], dn69=[MigrateTask{from='dn6'\n" +
                ", to='dn69'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=46, valueEnd=46)]\n" +
                "}], dn70=[MigrateTask{from='dn6'\n" +
                ", to='dn70'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=47, valueEnd=47)]\n" +
                "}], dn71=[MigrateTask{from='dn7'\n" +
                ", to='dn71'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=49, valueEnd=49)]\n" +
                "}], dn72=[MigrateTask{from='dn7'\n" +
                ", to='dn72'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=50, valueEnd=50)]\n" +
                "}], dn73=[MigrateTask{from='dn7'\n" +
                ", to='dn73'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=51, valueEnd=51)]\n" +
                "}], dn74=[MigrateTask{from='dn7'\n" +
                ", to='dn74'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=52, valueEnd=52)]\n" +
                "}], dn75=[MigrateTask{from='dn7'\n" +
                ", to='dn75'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=53, valueEnd=53)]\n" +
                "}], dn76=[MigrateTask{from='dn7'\n" +
                ", to='dn76'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=54, valueEnd=54)]\n" +
                "}], dn77=[MigrateTask{from='dn7'\n" +
                ", to='dn77'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=80, valueEnd=80)]\n" +
                "}], dn78=[MigrateTask{from='dn7'\n" +
                ", to='dn78'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=81, valueEnd=81)]\n" +
                "}], dn79=[MigrateTask{from='dn7'\n" +
                ", to='dn79'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=82, valueEnd=82)]\n" +
                "}], dn80=[MigrateTask{from='dn7'\n" +
                ", to='dn80'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=83, valueEnd=83)]\n" +
                "}], dn81=[MigrateTask{from='dn8'\n" +
                ", to='dn81'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=85, valueEnd=85)]\n" +
                "}], dn82=[MigrateTask{from='dn8'\n" +
                ", to='dn82'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=86, valueEnd=86)]\n" +
                "}], dn83=[MigrateTask{from='dn8'\n" +
                ", to='dn83'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=87, valueEnd=87)]\n" +
                "}], dn84=[MigrateTask{from='dn8'\n" +
                ", to='dn84'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=88, valueEnd=88)]\n" +
                "}], dn85=[MigrateTask{from='dn8'\n" +
                ", to='dn85'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=0, valueEnd=0)]\n" +
                "}], dn86=[MigrateTask{from='dn8'\n" +
                ", to='dn86'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=1, valueEnd=1)]\n" +
                "}], dn87=[MigrateTask{from='dn8'\n" +
                ", to='dn87'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=2, valueEnd=2)]\n" +
                "}], dn88=[MigrateTask{from='dn8'\n" +
                ", to='dn88'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=3, valueEnd=3)]\n" +
                "}], dn89=[MigrateTask{from='dn8'\n" +
                ", to='dn89'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=4, valueEnd=4)]\n" +
                "}], dn90=[MigrateTask{from='dn8'\n" +
                ", to='dn90'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=5, valueEnd=5)]\n" +
                "}], dn91=[MigrateTask{from='dn9'\n" +
                ", to='dn91'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=7, valueEnd=7)]\n" +
                "}], dn92=[MigrateTask{from='dn9'\n" +
                ", to='dn92'\n" +
                ", slots=[NodeIndexRange(nodeIndex=0, valueStart=8, valueEnd=8)]\n" +
                "}], dn93=[MigrateTask{from='dn9'\n" +
                ", to='dn93'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=40, valueEnd=40)]\n" +
                "}], dn94=[MigrateTask{from='dn9'\n" +
                ", to='dn94'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=41, valueEnd=41)]\n" +
                "}], dn95=[MigrateTask{from='dn9'\n" +
                ", to='dn95'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=42, valueEnd=42)]\n" +
                "}], dn96=[MigrateTask{from='dn9'\n" +
                ", to='dn96'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=43, valueEnd=43)]\n" +
                "}], dn97=[MigrateTask{from='dn9'\n" +
                ", to='dn97'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=44, valueEnd=44)]\n" +
                "}], dn98=[MigrateTask{from='dn9'\n" +
                ", to='dn98'\n" +
                ", slots=[NodeIndexRange(nodeIndex=1, valueStart=45, valueEnd=45)]\n" +
                "}], dn99=[MigrateTask{from='dn9'\n" +
                ", to='dn99'\n" +
                ", slots=[NodeIndexRange(nodeIndex=2, valueStart=66, valueEnd=66)]\n" +
                "}]}",tasks2.toString());
    }

}