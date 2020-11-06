package io.mycat.calcite.prepare;

import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.util.Pair;

import java.text.NumberFormat;
import java.util.*;

/** Listener for counting the attempts of each rule. Only enabled under DEBUG level.*/
  public class MycatRuleAttemptsListener implements RelOptListener {
    private long beforeTimestamp;
    private Map<String, Pair<Long, Long>> ruleAttempts;

    MycatRuleAttemptsListener() {
      ruleAttempts = new HashMap<>();
    }

    @Override public void relEquivalenceFound(RelEquivalenceEvent event) {
    }

    @Override public void ruleAttempted(RuleAttemptedEvent event) {
      if (event.isBefore()) {
        this.beforeTimestamp = System.nanoTime();
      } else {
        long elapsed = (System.nanoTime() - this.beforeTimestamp) / 1000;
        String rule = event.getRuleCall().getRule().toString();
        if (ruleAttempts.containsKey(rule)) {
          Pair<Long, Long> p = ruleAttempts.get(rule);
          ruleAttempts.put(rule, Pair.of(p.left + 1, p.right + elapsed));
        } else {
          ruleAttempts.put(rule, Pair.of(1L,  elapsed));
        }
      }
    }

    @Override public void ruleProductionSucceeded(RuleProductionEvent event) {
    }

    @Override public void relDiscarded(RelDiscardedEvent event) {
    }

    @Override public void relChosen(RelChosenEvent event) {
    }

    public String dump() {
      // Sort rules by number of attempts descending, then by rule elapsed time descending,
      // then by rule name ascending.
      List<Map.Entry<String, Pair<Long, Long>>> list =
          new ArrayList<>(this.ruleAttempts.entrySet());
      Collections.sort(list,
          (left, right) -> {
            int res = right.getValue().left.compareTo(left.getValue().left);
            if (res == 0) {
              res = right.getValue().right.compareTo(left.getValue().right);
            }
            if (res == 0) {
              res = left.getKey().compareTo(right.getKey());
            }
            return res;
          });

      // Print out rule attempts and time
      StringBuilder sb = new StringBuilder();
      sb.append(String
          .format(Locale.ROOT, "%n%-60s%20s%20s%n", "Rules", "Attempts", "Time (us)"));
      NumberFormat usFormat = NumberFormat.getNumberInstance(Locale.US);
      for (Map.Entry<String, Pair<Long, Long>> entry : list) {
        sb.append(
            String.format(Locale.ROOT, "%-60s%20s%20s%n",
                entry.getKey(),
                usFormat.format(entry.getValue().left),
                usFormat.format(entry.getValue().right)));
      }
      return sb.toString();
    }
  }