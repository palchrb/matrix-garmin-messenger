package connector

import (
	"context"
	"fmt"
	"os"

	gm "github.com/palchrb/garmin-messenger-go"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

// GarminLogin implements the two-step SMS OTP login flow using
// the HermesAuth client (github.com/palchrb/garmin-messenger-go).
//
//	Step 1: User provides phone number → gm.RequestOTP → Garmin sends SMS
//	Step 2: User provides OTP code   → gm.ConfirmOTP → session saved to disk
type GarminLogin struct {
	connector *GarminConnector
	user      *bridgev2.User

	// Set after step 1.
	phone  string
	auth   *gm.HermesAuth   // created in step 1, reused in step 2
	otpReq *gm.OtpRequest  // returned by RequestOTP, needed by ConfirmOTP
}

var _ bridgev2.LoginProcessUserInput = (*GarminLogin)(nil)

// Start returns Step 1: ask for phone number.
func (gl *GarminLogin) Start(_ context.Context) (*bridgev2.LoginStep, error) {
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       "fi.example.garmin.enter_phone",
		Instructions: "Enter the phone number you registered with the Garmin Messenger app (E.164 format, e.g. +12125551234).",
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{{
				Type:    bridgev2.LoginInputFieldTypePhoneNumber,
				ID:      "phone",
				Name:    "Phone number",
				Pattern: `^\+[1-9]\d{6,14}$`,
			}},
		},
	}, nil
}

// SubmitUserInput dispatches to the correct step.
func (gl *GarminLogin) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	if gl.phone == "" {
		return gl.submitPhone(ctx, input)
	}
	return gl.submitOTP(ctx, input)
}

// submitPhone — Step 1: create a HermesAuth for this phone, request SMS OTP.
func (gl *GarminLogin) submitPhone(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	phone := input["phone"]
	if phone == "" {
		return nil, fmt.Errorf("phone number is required")
	}

	// Determine the session directory for this login.
	loginID := loginIDFromPhone(phone)
	sessDir := gl.connector.sessionDir(loginID)

	// Ensure the session directory exists before HermesAuth tries to write to it.
	if err := os.MkdirAll(sessDir, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create session directory %s: %w", sessDir, err)
	}

	auth := gm.NewHermesAuth(gm.WithSessionDir(sessDir))

	// RequestOTP returns an OTPRequest that must be passed back to ConfirmOTP.
	// "Matrix Garmin Bridge" is the app name sent to the Garmin API — it
	// appears in the SMS: "Your code for Matrix Garmin Bridge is 123456".
	otpReq, err := auth.RequestOTP(ctx, phone, "Matrix Garmin Bridge")
	if err != nil {
		return nil, fmt.Errorf("failed to request SMS code: %w", err)
	}

	gl.phone = phone
	gl.auth = auth
	gl.otpReq = otpReq

	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       "fi.example.garmin.enter_otp",
		Instructions: fmt.Sprintf("A 6-digit code has been sent to %s via SMS. Enter it below.", phone),
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{{
				// LoginInputFieldTypePassword hides input in clients.
				Type:    bridgev2.LoginInputFieldTypePassword,
				ID:      "code",
				Name:    "6-digit SMS code",
				Pattern: `^\d{6}$`,
			}},
		},
	}, nil
}

// submitOTP — Step 2: confirm the OTP, saving the session to disk.
func (gl *GarminLogin) submitOTP(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	code := input["code"]
	if code == "" {
		return nil, fmt.Errorf("SMS code is required")
	}

	// ConfirmOTP validates the code and saves credentials to disk via HermesAuth.
	if err := gl.auth.ConfirmOTP(ctx, gl.otpReq, code); err != nil {
		return nil, fmt.Errorf("invalid SMS code: %w", err)
	}

	return gl.finishLogin(ctx)
}

// finishLogin creates the UserLogin record in the bridge database and
// immediately starts the SignalR connection.
func (gl *GarminLogin) finishLogin(ctx context.Context) (*bridgev2.LoginStep, error) {
	ul, err := gl.user.NewLogin(ctx, &database.UserLogin{
		ID:         loginIDFromPhone(gl.phone),
		RemoteName: gl.phone,
		Metadata: &UserLoginMetadata{
			PhoneNumber: gl.phone,
		},
	}, &bridgev2.NewLoginParams{
		LoadUserLogin: func(_ context.Context, login *bridgev2.UserLogin) error {
			login.Client = newGarminClient(gl.connector, login, gl.auth, gl.phone)
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to save login: %w", err)
	}

	// The bridge framework does not call Connect() after NewLogin —
	// it only calls Connect() on startup (StartLogins). We must start
	// the SignalR connection explicitly so the bridge works immediately
	// without requiring a restart.
	ul.Client.Connect(ul.Log.WithContext(context.Background()))

	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       "fi.example.garmin.complete",
		Instructions: fmt.Sprintf("Successfully logged in as %s", gl.phone),
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: ul.ID,
			UserLogin:   ul,
		},
	}, nil
}

// Cancel is called if the user aborts. No open connections to tear down.
func (gl *GarminLogin) Cancel() {}
