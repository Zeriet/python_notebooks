/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.pipeline;

import java.util.ArrayList;
import java.util.List;

public class PipeAdapter<T, U> {
    private final Pipe<T, U> pipe;
    private final List<Tap<U>> taps = new ArrayList<>();

    public PipeAdapter(Pipe pipe) {
        this.pipe = pipe;
    }

    public PipeAdapter<T, U> addTap(Tap<U> tap) {
        taps.add(tap);
        return this;
    }

    public U process(T in) {
        U out = pipe.process(in);
        taps.forEach(tap -> tap.process(out));
        return out;
    }
}
