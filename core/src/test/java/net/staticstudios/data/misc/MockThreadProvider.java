package net.staticstudios.data.misc;

import net.staticstudios.utils.ShutdownStage;
import net.staticstudios.utils.ShutdownTask;
import net.staticstudios.utils.ThreadUtilProvider;
import net.staticstudios.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class MockThreadProvider implements ThreadUtilProvider {
    private final ExecutorService mainThreadExecutorService;
    private final List<Runnable> syncOnDisableTasksRunNext = Collections.synchronizedList(new ArrayList<>());
    private final List<ShutdownTask> shutdownTasks = Collections.synchronizedList(new ArrayList<>());
    private final Logger logger = LoggerFactory.getLogger(MockThreadProvider.class.getName());
    private ExecutorService executorService;
    private boolean isShuttingDown = false;
    private boolean doneShuttingDown = false;


    public MockThreadProvider() {
        this.mainThreadExecutorService = Executors.newSingleThreadExecutor();
        this.executorService = Executors.newCachedThreadPool((r) -> new Thread(r, "MockThreadProvider"));
    }

    @Override
    public void submit(Runnable runnable) {
        if (doneShuttingDown) {
            throw new IllegalStateException("Cannot submit tasks after shutdown");
        }
        executorService.submit(() -> {
            try {
                runnable.run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void runSync(Runnable runnable) {
        if (isShuttingDown) {
            syncOnDisableTasksRunNext.add(runnable);
            return;
        }

        mainThreadExecutorService.submit(runnable);
    }

    @Override
    public void onShutdownRunSync(ShutdownStage shutdownStage, Runnable runnable) {
        shutdownTasks.add(new ShutdownTask(shutdownStage, () -> {
            ThreadUtils.safe(runnable);
            return null;
        }, true));
    }

    @Override
    public void onShutdownRunAsync(ShutdownStage shutdownStage, Supplier<CompletableFuture<Void>> task) {
        shutdownTasks.add(new ShutdownTask(shutdownStage, task, false));

    }

    @Override
    public boolean isShuttingDown() {
        return isShuttingDown;
    }

    public void shutdown() {
        isShuttingDown = true;

        executorService.shutdown();
        try {
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Map<ShutdownStage, List<ShutdownTask>> tasks = new HashMap<>();
        shutdownTasks.forEach(task -> tasks.computeIfAbsent(task.stage(), k -> new ArrayList<>()).add(task));

        ShutdownStage.getStages()
                .forEach(stage -> {
                    if (tasks.containsKey(stage)) {
                        getLogger().info("Running shutdown tasks for stage " + stage);

                        List<CompletableFuture<Void>> asyncFutures = new ArrayList<>();
                        List<Runnable> syncTasks = new ArrayList<>();

                        tasks.get(stage).forEach(task -> {
                            if (task.sync()) {
                                syncTasks.add(() -> task.task().get());
                            } else {
                                asyncFutures.add(task.task().get());
                            }
                        });

                        //Wait for all async tasks to finish
                        try {
                            CompletableFuture.allOf(asyncFutures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            getLogger().error("Failed to wait for async tasks to finish during shutdown stage " + stage);
                            e.printStackTrace();
                        }

                        syncTasks.forEach(Runnable::run);

                        syncOnDisableTasksRunNext.forEach(Runnable::run);

                        syncOnDisableTasksRunNext.clear();
                    }
                });

        doneShuttingDown = true;
    }

    private Logger getLogger() {
        return logger;
    }
}