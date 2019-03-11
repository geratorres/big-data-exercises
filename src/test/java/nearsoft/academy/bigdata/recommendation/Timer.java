package nearsoft.academy.bigdata.recommendation;

public class Timer {

    private long startTime = time();
    private String timerLabel = "Timer";

    public void log(Object... objects) {
        for (Object o : objects) {
            System.out.println(o);
        }
    }

    private long time() {
        return System.nanoTime();
    }

    public void start() {
        start(null);
    }

    public void start(String label) {
        timerLabel = label;
        log(label + "...");
        startTime = time();
    }

    public void stop() {
        stop(" ");
    }

    public void stop(String prefix) {
        int divider = 1000000000;
        switch (prefix.charAt(0)) {
            case 'm':
                divider = 1000000;
                break;
            case 'u':
                divider = 1000;
                break;
            case 'n':
                divider = 1000;
                break;
            default:
                prefix = "";
        }
        log(timerLabel + ": " + ((double) (time() - startTime) / divider) + prefix + "s");
    }
}
