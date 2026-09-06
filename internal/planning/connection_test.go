package planning

import (
	"context"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"testing"
)

type connectionPlannerModel struct {
	plannerModel
	account string
}

func (m *connectionPlannerModel) Descriptor() Descriptor {
	descriptor := m.plannerModel.Descriptor()
	descriptor.ConnectionID = "configured"
	return descriptor
}
func (m *connectionPlannerModel) CompleteRequest(ctx context.Context, request modelinput.Request) (TextCompletion, error) {
	result, err := m.plannerModel.CompleteRequest(ctx, request)
	result.Usage.ConnectionID = m.account
	return result, err
}
func TestPlannerRequiresConfiguredUsageAccount(t *testing.T) {
	for _, account := range []string{"configured", "other", ""} {
		model := &connectionPlannerModel{plannerModel: plannerModel{text: `{"tasks":[]}`}, account: account}
		planner, err := NewModelPlanner(model)
		if err != nil {
			t.Fatal(err)
		}
		_, err = planner.Build(planningTestContext(t), Input{Intent: acceptedDraft()}, core.ExecutionAgent)
		if (err == nil) != (account == "configured") {
			t.Fatalf("account=%q err=%v", account, err)
		}
	}
}
