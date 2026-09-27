package ledger

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentReadRequiresLinkGuards(t *testing.T) {
	for _, change := range []string{"missing", "replaced", "extra unique index", "extra source trigger", "temporary source trigger", "temporary link table"} {
		t.Run(change, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			correlation := appendDerivedIncidentChain(t, store, 2)
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256); err != nil {
				t.Fatalf("valid history: %v", err)
			}
			statement := `DROP TRIGGER incident_event_links_delete_guard`
			switch change {
			case "replaced":
				statement += `; CREATE TRIGGER incident_event_links_delete_guard BEFORE DELETE ON incident_event_links BEGIN SELECT 1; END`
			case "extra unique index":
				statement = `CREATE UNIQUE INDEX unwanted_link_constraint ON incident_event_links(target_kind,target_id,event_sequence)`
			case "extra source trigger":
				statement = `CREATE TRIGGER skip_link_maintenance AFTER INSERT ON events BEGIN SELECT RAISE(IGNORE); END`
			case "temporary source trigger":
				statement = `CREATE TEMP TRIGGER skip_link_maintenance AFTER INSERT ON main.events BEGIN SELECT RAISE(IGNORE); END`
			case "temporary link table":
				statement = `CREATE TEMP TABLE incident_event_links AS SELECT * FROM main.incident_event_links`
			}
			if _, err := store.db.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
			// Even rewriting the mutable schema fingerprint cannot make an
			// incomplete maintenance contract acceptable to the public reader.
			fingerprint, err := storageSchemaFingerprint(t.Context(), store.db)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE agentos_storage SET schema_fingerprint=?`, fingerprint); err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
			if err == nil || !strings.Contains(err.Error(), "incident link schema") {
				t.Fatalf("read with %s guard: %v", change, err)
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("invalid index returned partial evidence")
			}
		})
	}
}
