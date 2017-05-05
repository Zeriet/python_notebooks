/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.pipeline;

import java.util.ArrayList;
import java.util.List;

public class Pipeline {
    private List<PipeAdapter> pipes;

    protected Pipeline(List<PipeAdapter> pipes) {
        this.pipes = pipes;
    }

    public void flow(Object data) {
        for (PipeAdapter pipe : pipes) {
            data = pipe.process(data);
        }
    }

    public static class Builder<T> {

        private PipeAdapter pipeAdapter;
        private List<PipeAdapter> pipes = new ArrayList<>();

        public Builder() {
        }

        public Builder(List<PipeAdapter> pipes) {
            this.pipes = pipes;
        }

        public <U> Builder<U> chain(Pipe<T, U> pipe) {
            if (pipeAdapter != null) {
                pipes.add(pipeAdapter);
            }
            pipeAdapter = new PipeAdapter(pipe);
            return new Builder<>(pipes);
        }

        public Builder<T> tap(Tap<T> tap) {
            pipeAdapter.addTap(tap);
            return this;
        }

        public Pipeline build() {
            return new Pipeline(pipes);
        }

    }
}
