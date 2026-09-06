package modelinput

import "testing"

func TestInvocationContextCannotSilentlyUseMissingScope(t *testing.T) {
	if _, err := BindContext(t.Context(), requestForTest()); err == nil {
		t.Fatal("bound without admitted invocation")
	}
	ctx, err := WithInvocation(t.Context(), "org-1", "execution-1")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := BindContext(ctx, requestForTest())
	if err != nil {
		t.Fatal(err)
	}
	scope, err := Invocation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateBinding(scope, binding.Request()); err != nil {
		t.Fatal(err)
	}
	other, err := InvocationScope("org-2", "execution-1")
	if err != nil {
		t.Fatal(err)
	}
	if ValidateBinding(other, binding.Request()) == nil {
		t.Fatal("cross-organization binding accepted")
	}
	request := binding.Request()
	request.Messages[1].Source.Handle = ""
	if ValidateBinding(scope, request) == nil {
		t.Fatal("missing handle accepted")
	}
	request = binding.Request()
	request.Messages[1].Text = "new content"
	request.Messages[1].Source.Digest = TextDigest("new content")
	if ValidateBinding(scope, request) == nil {
		t.Fatal("new content reused old handle")
	}
}
