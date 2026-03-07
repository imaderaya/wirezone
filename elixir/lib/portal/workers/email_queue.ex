defmodule Portal.Workers.EmailQueue do
  @moduledoc """
  Oban worker that delivers queued emails at a rate within ACS limits.

  Runs every minute. Sends up to 30 emails per run, respecting a 100/hr
  global cap. :now priority emails are sent before :later priority emails.

  Permanent failures (4xx) transition the row to :failed.
  Transient failures (429, 5xx, network) transition to :errored and are
  retried after a 5-minute backoff.
  """

  use Oban.Worker,
    queue: :email_queue,
    max_attempts: 3,
    unique: [period: :infinity, states: [:available, :scheduled, :executing, :retryable]]

  alias Portal.{Mailer, EmailQueue}
  alias __MODULE__.Database
  require Logger

  @rate_limit_per_minute 30
  @rate_limit_per_hour 100

  @impl Oban.Worker
  def perform(_job) do
    sent_this_hour = Database.count_sent_last_hour()
    remaining_hour = @rate_limit_per_hour - sent_this_hour

    if remaining_hour <= 0 do
      Logger.warning("Email queue hourly rate limit reached, skipping batch")
      :ok
    else
      sent_this_minute = Database.count_sent_last_minute()
      remaining_minute = @rate_limit_per_minute - sent_this_minute
      batch_size = min(remaining_minute, remaining_hour)

      if batch_size <= 0 do
        Logger.debug("Email queue per-minute rate limit reached, skipping batch")
        :ok
      else
        Database.fetch_pending(batch_size)
        |> Enum.each(&process_email/1)

        :ok
      end
    end
  end

  defp process_email(%EmailQueue{} = entry) do
    Database.mark_attempted(entry)
    email = deserialize(entry.request, entry.id)

    case Mailer.deliver(email) do
      {:ok, response} ->
        Database.mark_accepted(entry, response)

      {:error, {status, body}} when is_integer(status) and status in 400..499 ->
        Logger.error("Email delivery permanently failed",
          id: entry.id,
          account_id: entry.account_id,
          status: status,
          body: inspect(body)
        )

        Database.mark_failed(entry, %{"status" => status, "body" => inspect(body)})

      {:error, reason} ->
        Logger.error("Email delivery failed (retryable)",
          id: entry.id,
          account_id: entry.account_id,
          reason: inspect(reason)
        )

        Database.mark_errored(entry, %{"reason" => inspect(reason)})
    end
  end

  defp deserialize(
         %{
           "to" => to,
           "from" => from,
           "subject" => subject,
           "html_body" => html_body,
           "text_body" => text_body
         },
         operation_id
       ) do
    Swoosh.Email.new()
    |> Swoosh.Email.to(Enum.map(to, fn %{"name" => n, "address" => a} -> {n, a} end))
    |> Swoosh.Email.from({from["name"], from["address"]})
    |> Swoosh.Email.subject(subject)
    |> Swoosh.Email.html_body(html_body)
    |> Swoosh.Email.text_body(text_body)
    |> Map.put(:provider_options, %{operation_id: operation_id})
  end

  defmodule Database do
    import Ecto.Query
    alias Portal.{Safe, EmailQueue}

    def fetch_pending(limit) do
      from(e in EmailQueue,
        where: e.status in [:pending, :errored],
        where: is_nil(e.last_attempted_at) or e.last_attempted_at < ago(5, "minute"),
        order_by: [
          asc: fragment("CASE WHEN ? = 'now' THEN 0 ELSE 1 END", e.priority),
          asc: e.inserted_at
        ],
        limit: ^limit
      )
      |> Safe.unscoped()
      |> Safe.all()
    end

    def count_sent_last_hour do
      from(e in EmailQueue,
        where: e.status == :accepted,
        where: e.last_attempted_at > ago(1, "hour")
      )
      |> Safe.unscoped(:replica)
      |> Safe.aggregate(:count)
    end

    def count_sent_last_minute do
      from(e in EmailQueue,
        where: e.status == :accepted,
        where: e.last_attempted_at > ago(1, "minute")
      )
      |> Safe.unscoped(:replica)
      |> Safe.aggregate(:count)
    end

    def mark_attempted(%EmailQueue{} = entry) do
      entry
      |> Ecto.Changeset.change(last_attempted_at: DateTime.utc_now())
      |> Safe.unscoped()
      |> Safe.update()
    end

    def mark_accepted(%EmailQueue{} = entry, response) do
      entry
      |> Ecto.Changeset.change(status: :accepted, response: response)
      |> Safe.unscoped()
      |> Safe.update()
    end

    def mark_failed(%EmailQueue{} = entry, response) do
      entry
      |> Ecto.Changeset.change(status: :failed, response: response)
      |> Safe.unscoped()
      |> Safe.update()
    end

    def mark_errored(%EmailQueue{} = entry, response) do
      entry
      |> Ecto.Changeset.change(status: :errored, response: response)
      |> Safe.unscoped()
      |> Safe.update()
    end
  end
end
