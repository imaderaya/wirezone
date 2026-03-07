defmodule Portal.Repo.Migrations.CreateEmailQueue do
  use Ecto.Migration

  def change do
    create table(:email_queue, primary_key: false) do
      add(:account_id, references(:accounts, type: :binary_id, on_delete: :delete_all),
        null: false,
        primary_key: true
      )

      add(:id, :binary_id, null: false, primary_key: true)
      add(:priority, :string, null: false)
      add(:status, :string, null: false, default: "pending")
      add(:request, :map, null: false)
      add(:response, :map)
      add(:last_attempted_at, :utc_datetime_usec)

      timestamps(type: :utc_datetime_usec)
    end

    create(index(:email_queue, [:inserted_at]))
    create(index(:email_queue, [:status, :last_attempted_at]))
  end
end
