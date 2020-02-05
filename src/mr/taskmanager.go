package mr

import (
	"sync"
	"time"
)

type TaskState int

const (
	TaskStateIdle TaskState = iota
	TaskStateInProgress
	TaskStateCompleted
)

type TaskType int

const (
	TaskTypeMap TaskType = iota
	TaskTypeReduce
)

type Task struct {
	ID    int
	State TaskState
	Input []string
	timer *time.Timer
}

// NewTask creates a new task
func NewTask(id int, input []string) *Task {
	return &Task{
		ID:    id,
		State: TaskStateIdle,
		Input: input,
	}
}

type TaskManager struct {
	lock    sync.RWMutex
	cond    sync.Cond
	tasks   []*Task
	timeout time.Duration
}

// NewTaskManager creates a TaskManager with inputs
func NewTaskManager(inputs [][]string, timeout time.Duration) *TaskManager {
	tm := &TaskManager{
		timeout: timeout,
	}

	tasks := make([]*Task, len(inputs))
	for idx, input := range inputs {
		t := NewTask(idx, input)
		tasks[idx] = t
	}

	tm.cond.L = &tm.lock
	tm.tasks = tasks
	return tm
}

func (tm *TaskManager) getIdleTasks() []*Task {
	var idleTasks []*Task
	for _, t := range tm.tasks {
		if t.State == TaskStateIdle {
			idleTasks = append(idleTasks, t)
		}
	}
	return idleTasks
}

// Done returns true if all the task are done
func (tm *TaskManager) Done() bool {
	for _, t := range tm.tasks {
		if t.State != TaskStateCompleted {
			return false
		}
	}

	return true
}

// Trigger will block the function until choose a idle task or all tasks done
func (tm *TaskManager) Trigger() *Task {
	tm.lock.Lock()
	defer tm.lock.Unlock()

	for {
		for len(tm.getIdleTasks()) == 0 {
			if tm.Done() {
				return nil
			}

			tm.cond.Wait()
		}
		idleTasks := tm.getIdleTasks()
		t := idleTasks[0]
		t.State = TaskStateInProgress
		if t.timer == nil {
			t.timer = time.AfterFunc(tm.timeout, func() {
				tm.lock.Lock()
				defer tm.lock.Unlock()

				if t.State == TaskStateInProgress {
					t.State = TaskStateIdle
					tm.cond.Broadcast()
				}
			})
		} else {
			t.timer.Reset(tm.timeout)
		}
		return t
	}
}

// Complete mark the tark with id as completed
// return false if the task already done or not exist
func (tm *TaskManager) Complete(id int) bool {
	tm.lock.Lock()
	defer tm.lock.Unlock()

	for _, t := range tm.tasks {
		if t.ID == id && t.State == TaskStateInProgress {
			t.timer.Stop()
			t.State = TaskStateCompleted
			tm.cond.Broadcast()
			return true
		}
	}

	return false
}

func (tm *TaskManager) UpdateTaskInputsByID(id int, inputs []string) bool {
	tm.lock.Lock()
	defer tm.lock.Unlock()

	for _, t := range tm.tasks {
		if t.ID == id {
			t.Input = append(t.Input, inputs...)
			return true
		}
	}

	return false
}
