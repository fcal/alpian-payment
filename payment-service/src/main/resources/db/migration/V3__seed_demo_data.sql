-- Demo data so the service is usable immediately after `bootRun`. There is no user-creation
-- or account-opening API: account onboarding is out of scope for this exercise, and seeding
-- keeps the focus on the payment path.
--
-- Fixed UUIDs so the README can document working curl examples. Component tests create their
-- own isolated fixtures rather than relying on these.

INSERT INTO app_user (id, name) VALUES
    ('11111111-1111-1111-1111-111111111111', 'Ada Lovelace'),
    ('22222222-2222-2222-2222-222222222222', 'Alan Turing');

INSERT INTO account (id, user_id, balance, currency) VALUES
    -- Ada holds two accounts, demonstrating the one-user-to-many-accounts cardinality.
    ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1', '11111111-1111-1111-1111-111111111111', 1000.0000, 'CHF'),
    ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2', '11111111-1111-1111-1111-111111111111',  250.5000, 'EUR'),
    ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1', '22222222-2222-2222-2222-222222222222',  500.0000, 'CHF');
